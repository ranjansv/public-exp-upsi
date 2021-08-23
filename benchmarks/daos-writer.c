#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <daos/tests_lib.h>
#include <daos.h>
#include "suite/daos_test.h"
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
array(size_t arr_size_mb, int steps)
{
	daos_handle_t	 oh;
	struct io_req	*reqs;
	int		 rc;
	int		 iter;

	size_t data_per_rank = arr_size_mb * MB_in_bytes / procs;


	/** allocate and initialize I/O requests */
	D_ALLOC_ARRAY(data, data_per_rank);
	D_ALLOC_ARRAY(reqs, MAX_IOREQS);
	ASSERT(reqs != NULL, "malloc of reqs failed");
	ioreqs_init(reqs, data_per_rank);


	/** Transactional overwrite of the array at each iteration */
	for (iter = 0; iter < steps; iter++) {
	    
	    MPI_Barrier(comm);
	    if(rank == 0) {
		epoch++;
	        daos_cont_create_snap(coh, &epoch, NULL, NULL);
	        ASSERT(rc == 0, "daos_cont_create_snap failed with %d", rc);
	    }
	    MPI_Barrier(comm);
	}

	if(rank == 0)
	   print_message("rank 0 array()..completed\n");

	D_FREE(reqs);
	D_FREE(data);
}


int
main(int argc, char **argv)
{
	int	rc;
        uuid_parse(argv[1], pool_uuid); 
	size_t arr_size_mb = atoi(argv[2]);
	int steps = atoi(argv[3]);

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
		//pool_create();

		/** connect to the just created DAOS pool */
		rc = daos_pool_connect(pool_uuid, DSS_PSETID,
				       //DAOS_PC_EX ,
				       DAOS_PC_RW /* read write access */,
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
                rc = daos_cont_create(poh, co_uuid, NULL /* properties */,
                                      NULL /* event */);
                ASSERT(rc == 0, "container create failed with %d", rc);

                /** open container */
                rc = daos_cont_open(poh, co_uuid, DAOS_COO_RW, &coh, NULL,
                                    NULL);
                ASSERT(rc == 0, "container open failed with %d", rc);


	}

	/** share container handle with peer tasks */
	handle_share(&coh, HANDLE_CO, rank, poh, 1);

        /** the other tasks write the array */
        array(arr_size_mb, steps);

	/** close container */
	daos_cont_close(coh, NULL);

	/** destroy container */
	if(rank == 0) {
	    rc = daos_cont_destroy(poh, co_uuid, 1 /* force */, NULL);
	    ASSERT(rc == 0, "daos_cont_destroy failed with %d", rc);

	    print_message("rank 0 daos_cont_destroy()\n");
	}


	/** disconnect from pool & destroy it */
	daos_pool_disconnect(poh, NULL);
	//if (rank == 0)
		/** free allocated storage */
	//	pool_destroy();

	/** destroy event queue */
	rc = daos_eq_destroy(eq, 0);
	ASSERT(rc == 0, "eq destroy failed with %d", rc);

	/** shutdown the local DAOS stack */
	rc = daos_fini();
	ASSERT(rc == 0, "daos_fini failed with %d", rc);

	MPI_Finalize();
	return rc;
}
