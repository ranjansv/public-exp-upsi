#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


#include <daos/tests_lib.h>
#include <daos.h>
#include <daos_fs.h>
#include "suite/daos_test.h"
#include "suite/dfs_test.h"
#include <mpi.h>

/** local task information */
int			 rank = -1;
int			 wrank;
int 			 procs;
char			 node[128] = "unknown";

/* MPI communicator for writers */
MPI_Comm comm;

/** Name of the process set associated with the DAOS server */
#define	DSS_PSETID	 "daos_tier0"

#define MB_in_bytes    1048576

/** Event queue */
daos_handle_t	eq;

/** Pool information */
uuid_t			 pool_uuid;	/* only used on rank 0 */
d_rank_list_t		 svcl;		/* only used on rank 0 */
daos_handle_t		 poh;		/* shared pool handle */

/** Container information */
uuid_t			 co_uuid;	/* only used on rank 0 */
daos_handle_t		 coh;		/* shared container handle */
daos_epoch_t		 epoch;		/* epoch in-use */

/** File system handle */
dfs_t                    *dfs;          /* Pointer to the mounted file system. */
d_iov_t                  glob = { NULL, 0, 0 };          /* Shared file system handle */


/**
 * Array parameters
 * Each task overwrites a different section of the array at every iteration.
 * An epoch number is associated with each iteration. One task can have at
 * most MAX_IOREQS I/O requests in flight and then needs to wait for completion
 * of an request in flight before sending a new one.
 * The actual data written in the array is the epoch number.
 */
#define	MAX_IOREQS	1	   /* number of concurrent i/o reqs in flight */

/** an i/o request in flight */
struct io_req {

	d_iov_t	iov;
	d_sg_list_t	sg;

	daos_event_t	ev;
};


/** data buffer */
char *data;

#define FAIL(fmt, ...)						\
do {								\
	fprintf(stderr, "Process %d(%s): " fmt " aborting\n",	\
		rank, node, ## __VA_ARGS__);			\
	MPI_Abort(MPI_COMM_WORLD, 1);				\
} while (0)

#define	ASSERT(cond, ...)					\
do {								\
	if (!(cond))						\
		FAIL(__VA_ARGS__);				\
} while (0)

void
pool_create(void)
{
	int	rc;

	/**
	 * allocate list of service nodes, returned as output parameter of
	 * dmg_pool_create() and used to connect
	 */

	/** create pool over all the storage targets */
	svcl.rl_nr = 3;
	D_ALLOC_ARRAY(svcl.rl_ranks, svcl.rl_nr);
	ASSERT(svcl.rl_ranks);
	rc = dmg_pool_create(NULL /* config file */,
			     geteuid() /* user owner */,
			     getegid() /* group owner */,
			     DSS_PSETID /* daos server process set ID */,
			     NULL /* list of targets, NULL = all */,
			     10ULL << 30 /* target SCM size, 10G */,
			     40ULL << 30 /* target NVMe size, 40G */,
			     NULL /* pool props */,
			     &svcl /* pool service nodes */,
			     pool_uuid /* the uuid of the pool created */);
	ASSERT(rc == 0, "pool create failed with %d", rc);
}

void
pool_destroy(void)
{
	int	rc;

	/** destroy the pool created in pool_create */
	rc = dmg_pool_destroy(NULL, pool_uuid, DSS_PSETID, 1 /* force */);
	ASSERT(rc == 0, "pool destroy failed with %d", rc);
	D_FREE(svcl.rl_ranks);
}

static inline void
ioreqs_init(struct io_req *reqs, size_t data_per_rank) {
	int rc;
	int j;


	for (j = 0; j < MAX_IOREQS; j++) {
		struct io_req	*req = &reqs[j];

		/** initialize event */
		//rc = daos_event_init(&req->ev, eq, NULL);
		//ASSERT(rc == 0, "event init failed with %d", rc);

		/** initialize scatter/gather */
		req->iov = (d_iov_t) {
			.iov_buf	= data,
			.iov_buf_len	= data_per_rank * sizeof(data[0]),
			.iov_len	= data_per_rank * sizeof(data[0]),
		};
		req->sg.sg_nr		= 1;
		req->sg.sg_iovs		= &req->iov;
	}
}

void
array(size_t arr_size_mb, int steps)
{
	daos_handle_t	 oh;
	struct io_req	*reqs;
	int		 rc;
	int		 iter;

	dfs_obj_t       *obj; /* DAOS file object */
	daos_off_t      off;  /* Offset into the file to write to */


	size_t data_per_rank = arr_size_mb * MB_in_bytes / procs;


	/** allocate and initialize I/O requests */
	D_ALLOC_ARRAY(data, data_per_rank);
	D_ALLOC_ARRAY(reqs, MAX_IOREQS);
	ASSERT(reqs != NULL, "malloc of reqs failed");
	ioreqs_init(reqs, data_per_rank);

	char filename[20];

	sprintf(filename, "rank-%d", rank);

	rc = dfs_open(dfs, NULL, filename,  S_IFREG | S_IWUSR | S_IRUSR, O_CREAT|O_RDWR,
			0 /* cid */, 0 /* chunk size */, NULL /*value*/, &obj);
	ASSERT(rc == 0, "dfs_open failed with %d", rc);

	off = 0;

	/** Transactional overwrite of the array at each iteration */
	for (iter = 0; iter < steps; iter++) {
	    
	    dfs_write(dfs, obj, &reqs[0].sg, off, NULL);
	    off += data_per_rank;

	    MPI_Barrier(comm);
	    if(rank == 0) {
		epoch++;
		off = 0;
	        daos_cont_create_snap(coh, &epoch, NULL, NULL);
	    }
	    MPI_Barrier(comm);
	}

	D_FREE(reqs);
	D_FREE(data);
}


int
main(int argc, char **argv)
{
	int	rc;

	size_t arr_size_mb = atoi(argv[1]);
	int steps = atoi(argv[2]);

	rc = gethostname(node, sizeof(node));
	ASSERT(rc == 0, "buffer for hostname too small");

	rc = MPI_Init(&argc, &argv);
	ASSERT(rc == MPI_SUCCESS, "MPI_Init failed with %d", rc);

	MPI_Comm_size(MPI_COMM_WORLD, &wrank);

	const unsigned int color = 1;
	MPI_Comm_split(MPI_COMM_WORLD, color, wrank, &comm); 

	MPI_Comm_rank(comm, &rank);
	MPI_Comm_size(comm, &procs);


	/** initialize the local DAOS stack */
	rc = daos_init();
	ASSERT(rc == 0, "daos_init failed with %d", rc);

	/** create event queue */
	rc = daos_eq_create(&eq);
	ASSERT(rc == 0, "eq create failed with %d", rc);

	if (rank == 0) {
		/** create a test pool and container for this test */
		pool_create();

		/** connect to the just created DAOS pool */
		rc = daos_pool_connect(pool_uuid, DSS_PSETID, NULL /* svc */,
				       DAOS_PC_EX /* exclusive access */,
				       &poh /* returned pool handle */,
				       NULL /* returned pool info */,
				       NULL /* event */);
		ASSERT(rc == 0, "pool connect failed with %d", rc);
	}

	/** share pool handle with peer tasks */
	handle_share(&poh, HANDLE_POOL, rank, poh, 1);

	if (rank == 0) {
		/** generate uuid for container */
		uuid_generate(co_uuid);

		/** create container */
		rc = dfs_cont_create(poh, co_uuid, NULL /* properties */,
				      &coh, NULL);
		ASSERT(rc == 0, "container create failed with %d", rc);

		/** Mount a file system*/
		rc = dfs_mount(poh, coh, O_RDWR, &dfs);
		ASSERT(rc == 0, "DAOS file system failed with %d", rc);

		rc = dfs_local2global(dfs, &glob);
		ASSERT(rc == 0, "Convert dfs_local2global failed with %d", rc);

	}

	/** share container handle with peer tasks */
	handle_share(&coh, HANDLE_CO, rank, poh, 1);

        dfs_test_share(poh, coh, rank, &dfs);
        /** the other tasks write the array */
        array(arr_size_mb, steps);

	/** close container */
	daos_cont_close(coh, NULL);

	/** disconnect from pool & destroy it */
	daos_pool_disconnect(poh, NULL);
	if (rank == 0)
		/** free allocated storage */
		pool_destroy();

	/** destroy event queue */
	rc = daos_eq_destroy(eq, 0);
	ASSERT(rc == 0, "eq destroy failed with %d", rc);

	/** shutdown the local DAOS stack */
	rc = daos_fini();
	ASSERT(rc == 0, "daos_fini failed with %d", rc);

	MPI_Finalize();
	return rc;
}
