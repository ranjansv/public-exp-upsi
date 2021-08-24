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
#define	DSS_PSETID	 "daos_server"
//#define	DSS_PSETID	 "daos_tier0"

#define MB_in_bytes    1048576
#define NUM_ELEMS       64
static daos_ofeat_t feat = DAOS_OF_DKEY_UINT64 | DAOS_OF_KV_FLAT |
        DAOS_OF_ARRAY;
static daos_ofeat_t featb = DAOS_OF_DKEY_UINT64 | DAOS_OF_KV_FLAT |
        DAOS_OF_ARRAY | DAOS_OF_ARRAY_BYTE;

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

static inline void
ioreqs_init(struct io_req *reqs, size_t data_per_rank) {
       int rc;
       int j;


       for (j = 0; j < MAX_IOREQS; j++) {
               struct io_req   *req = &reqs[j];

               /** initialize event */
               //rc = daos_event_init(&req->ev, eq, NULL);
               //ASSERT(rc == 0, "event init failed with %d", rc);

               /** initialize scatter/gather */
               req->iov = (d_iov_t) {
                       .iov_buf        = data,
                       .iov_buf_len    = data_per_rank * sizeof(data[0]),
                       .iov_len        = data_per_rank * sizeof(data[0]),
               };
               req->sg.sg_nr           = 1;
               req->sg.sg_iovs         = &req->iov;
       }
}

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

static void
array_oh_share(daos_handle_t *oh)
{
        d_iov_t ghdl = { NULL, 0, 0 };
        int             rc;

        if (rank == 0) {
                /** fetch size of global handle */
                rc = daos_array_local2global(*oh, &ghdl);
                assert_rc_equal(rc, 0);
        }

        /** broadcast size of global handle to all peers */
        rc = MPI_Bcast(&ghdl.iov_buf_len, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);
        assert_int_equal(rc, MPI_SUCCESS);

        /** allocate buffer for global pool handle */
        D_ALLOC(ghdl.iov_buf, ghdl.iov_buf_len);
        ghdl.iov_len = ghdl.iov_buf_len;

        if (rank == 0) {
                /** generate actual global handle to share with peer tasks */
                rc = daos_array_local2global(*oh, &ghdl);
                assert_rc_equal(rc, 0);
        }

        /** broadcast global handle to all peers */
        rc = MPI_Bcast(ghdl.iov_buf, ghdl.iov_len, MPI_BYTE, 0, MPI_COMM_WORLD);
        assert_int_equal(rc, MPI_SUCCESS);

        if (rank != 0) {
                /** unpack global handle */
                rc = daos_array_global2local(coh, ghdl, 0, oh);
                assert_rc_equal(rc, 0);
        }

        D_FREE(ghdl.iov_buf);

        MPI_Barrier(MPI_COMM_WORLD);
}

void
read_data(size_t arr_size_mb, int steps, int async)
{
        daos_obj_id_t   oid;
        daos_handle_t   oh;
        daos_array_iod_t iod;
        daos_range_t    rg;
        d_sg_list_t     sgl;
        d_iov_t         iov;
        int             *wbuf = NULL, *rbuf = NULL;
        daos_size_t     i;
        daos_event_t    ev, *evp;
        int             rc;
        int             iter;

        /* Temporary assignment */
        daos_size_t cell_size = 1;
	static daos_size_t chunk_size = 16;

        MPI_Barrier(MPI_COMM_WORLD);
        /** create the array on rank 0 and share the oh. */
        if (rank == 0) {
                oid = daos_test_oid_gen(coh, OC_SX,
                                        (cell_size == 1) ? featb : feat, 0, 0);
                rc = daos_array_create(coh, oid, DAOS_TX_NONE, cell_size,
                                       chunk_size, &oh, NULL);
                assert_rc_equal(rc, 0);
        }
        array_oh_share(&oh);

        /** Allocate and set buffer */
        D_ALLOC_ARRAY(wbuf, NUM_ELEMS);
        assert_non_null(wbuf);
        D_ALLOC_ARRAY(rbuf, NUM_ELEMS);
        assert_non_null(rbuf);
        for (i = 0; i < NUM_ELEMS; i++)
                wbuf[i] = i+1;

        /** set array location */
        iod.arr_nr = 1;
        rg.rg_len = NUM_ELEMS * sizeof(int) / cell_size;
        rg.rg_idx = rank * rg.rg_len;
        iod.arr_rgs = &rg;

        /** set memory location */
        sgl.sg_nr = 1;
        d_iov_set(&iov, wbuf, NUM_ELEMS * sizeof(int));
        sgl.sg_iovs = &iov;

	for (iter = 0; iter < steps; iter++) {

        /** Read */
        if (async) {
                rc = daos_event_init(&ev, eq, NULL);
                assert_rc_equal(rc, 0);
        }
        d_iov_set(&iov, rbuf, NUM_ELEMS * sizeof(int));
        sgl.sg_iovs = &iov;
        rc = daos_array_read(oh, DAOS_TX_NONE, &iod, &sgl,
                             async ? &ev : NULL);
        assert_rc_equal(rc, 0);
        if (async) {
                /** Wait for completion */
                rc = daos_eq_poll(eq, 0, DAOS_EQ_WAIT, 1, &evp);
                assert_rc_equal(rc, 1);
                assert_ptr_equal(evp, &ev);
                assert_int_equal(evp->ev_error, 0);

                rc = daos_event_fini(&ev);
                assert_rc_equal(rc, 0);
        }

        /** Verify data */
        if (cell_size == 1)
                assert_int_equal(iod.arr_nr_short_read, 0);
        for (i = 0; i < NUM_ELEMS; i++) {
                if (wbuf[i] != rbuf[i]) {
                        printf("Data verification failed\n");
                        printf("%zu: written %d != read %d\n",
                                i, wbuf[i], rbuf[i]);
                }
                assert_int_equal(wbuf[i], rbuf[i]);
        }


        MPI_Barrier(MPI_COMM_WORLD);

        }
    D_FREE(rbuf);
    D_FREE(wbuf);

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
        //array(arr_size_mb, steps);
	read_data(arr_size_mb, steps, 0 /* Async I/O flag False*/);

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
