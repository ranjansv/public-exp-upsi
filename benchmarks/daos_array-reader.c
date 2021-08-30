#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "suite/daos_test.h"
#include <daos.h>
#include <daos/tests_lib.h>
#include <mpi.h>

/** local task information */
int rank = -1;
int wrank;
int procs;
char node[128] = "unknown";

/* MPI communicator for writers */
MPI_Comm comm;

/** Name of the process set associated with the DAOS server */
#define DSS_PSETID "daos_server"
//#define	DSS_PSETID	 "daos_tier0"
#define NUM_OBJS 1

#define MB_in_bytes 1048576
static daos_ofeat_t feat =
    DAOS_OF_DKEY_UINT64 | DAOS_OF_KV_FLAT | DAOS_OF_ARRAY;
static daos_ofeat_t featb =
    DAOS_OF_DKEY_UINT64 | DAOS_OF_KV_FLAT | DAOS_OF_ARRAY | DAOS_OF_ARRAY_BYTE;

/** Event queue */
daos_handle_t eq;

/** Pool information */
uuid_t pool_uuid;   /* only used on rank 0 */
d_rank_list_t svcl; /* only used on rank 0 */
daos_handle_t poh;  /* shared pool handle */

/** Container information */
uuid_t co_uuid;     /* only used on rank 0 */
daos_handle_t coh;  /* shared container handle */
daos_epoch_t epoch; /* epoch in-use */

/**
 * Array parameters
 * Each task overwrites a different section of the array at every iteration.
 * An epoch number is associated with each iteration. One task can have at
 * most MAX_IOREQS I/O requests in flight and then needs to wait for completion
 * of an request in flight before sending a new one.
 * The actual data written in the array is the epoch number.
 */
#define MAX_IOREQS 1 /* number of concurrent i/o reqs in flight */

/** an i/o request in flight */
struct io_req {

  d_iov_t iov;
  d_sg_list_t sg;

  daos_event_t ev;
};

/** data buffer */
char *data;

#define FAIL(fmt, ...)                                                         \
  do {                                                                         \
    fprintf(stderr, "Process %d(%s): " fmt " aborting\n", rank, node,          \
            ##__VA_ARGS__);                                                    \
    MPI_Abort(MPI_COMM_WORLD, 1);                                              \
  } while (0)

#define ASSERT(cond, ...)                                                      \
  do {                                                                         \
    if (!(cond))                                                               \
      FAIL(__VA_ARGS__);                                                       \
  } while (0)

static inline void ioreqs_init(struct io_req *reqs, size_t data_per_rank) {
  int rc;
  int j;

  for (j = 0; j < MAX_IOREQS; j++) {
    struct io_req *req = &reqs[j];

    /** initialize event */
    // rc = daos_event_init(&req->ev, eq, NULL);
    // ASSERT(rc == 0, "event init failed with %d", rc);

    /** initialize scatter/gather */
    req->iov = (d_iov_t){
        .iov_buf = data,
        .iov_buf_len = data_per_rank * sizeof(data[0]),
        .iov_len = data_per_rank * sizeof(data[0]),
    };
    req->sg.sg_nr = 1;
    req->sg.sg_iovs = &req->iov;
  }
}

void array(size_t arr_size_mb, int steps) {
  daos_handle_t oh;
  struct io_req *reqs;
  int rc;
  int iter;

  size_t data_per_rank = arr_size_mb * MB_in_bytes / procs;

  /** allocate and initialize I/O requests */
  D_ALLOC_ARRAY(data, data_per_rank);
  D_ALLOC_ARRAY(reqs, MAX_IOREQS);
  ASSERT(reqs != NULL, "malloc of reqs failed");
  ioreqs_init(reqs, data_per_rank);

  /** Transactional overwrite of the array at each iteration */
  for (iter = 0; iter < steps; iter++) {

    MPI_Barrier(comm);
    if (rank == 0) {
      epoch++;
      daos_cont_create_snap(coh, &epoch, NULL, NULL);
      ASSERT(rc == 0, "daos_cont_create_snap failed with %d", rc);
    }
    MPI_Barrier(comm);
  }

  if (rank == 0)
    print_message("rank 0 array()..completed\n");

  D_FREE(reqs);
  D_FREE(data);
}

static void array_oh_share(daos_handle_t *oh) {
  d_iov_t ghdl = {NULL, 0, 0};
  int rc;

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

void read_data(int procs, size_t arr_size_mb, int steps, int async) {
  daos_obj_id_t oid;
  daos_handle_t oh;
  daos_handle_t th;
  daos_array_iod_t iod;
  daos_range_t rg;
  d_sg_list_t sgl;
  d_iov_t iov;
  char *wbuf = NULL, *rbuf = NULL;
  daos_size_t i;
  daos_event_t ev, *evp;
  int rc;
  int iter;
  int num_elements;
  daos_size_t size;

  /* Temporary assignment */
  daos_size_t cell_size = 1;
  static daos_size_t chunk_size = 2097152;
  //static daos_size_t chunk_size = 16;
  daos_obj_id_t   oids[NUM_OBJS];
  uint32_t        oids_nr;
  daos_anchor_t anchor;

  int num_snapshots;
  char list_snapnames[steps][50]; 
  daos_epoch_t epochs[steps];

  oids_nr = 0;
  for(iter = 0; iter < NUM_OBJS; iter++) 
    memset(&oids[iter], 0, sizeof(daos_obj_id_t));

  if (rank == 0)
	  printf("arr_size_mb = %d\n", arr_size_mb);
    memset(&anchor, 0, sizeof(anchor));
    rc = daos_cont_list_snap(coh, &num_snapshots, epochs, list_snapnames, &anchor, NULL);
    ASSERT(rc == 0, "daos_cont_list_snap failed with %d", rc);

  num_elements = arr_size_mb * MB_in_bytes/ procs;
  D_ALLOC_ARRAY(wbuf, num_elements);
  assert_non_null(wbuf);
  D_ALLOC_ARRAY(rbuf, num_elements);
  assert_non_null(rbuf);

  /** set array location */
  iod.arr_nr = 1;
  rg.rg_len = num_elements * sizeof(char) / cell_size;
  rg.rg_idx = rank * rg.rg_len;
  iod.arr_rgs = &rg;

  /** set memory location */
  sgl.sg_nr = 1;
  d_iov_set(&iov, wbuf, num_elements * sizeof(char));
  sgl.sg_iovs = &iov;

  for (iter = 0; iter < steps; iter++) {
    MPI_Barrier(MPI_COMM_WORLD);

    printf("rank %d epoch: %lu\n", rank, epochs[iter]);

    //rc = daos_tx_open_snap(coh, epochs[iter], &th, NULL);
    //ASSERT(rc == 0, "daos_tx_open_snap failed with %d", rc);

    rc = daos_oit_open(coh, epochs[iter], &oh, NULL);
    ASSERT(rc == 0, "daos_oit_open failed with %d", rc);

    memset(&anchor, 0, sizeof(anchor));
    rc = daos_oit_list(oh, oids, &oids_nr, &anchor, NULL);
    ASSERT(rc == 0, "daos_oit_list failed with %d", rc);

    rc = daos_oit_close(oh, NULL);
    ASSERT(rc == 0, "daos_oit_close failed with %d", rc);

    MPI_Barrier(MPI_COMM_WORLD);
  }

  D_FREE(rbuf);
  D_FREE(wbuf);
}

int main(int argc, char **argv) {
  int rc;
  uuid_parse(argv[1], pool_uuid);
  uuid_parse(argv[2], co_uuid);
  size_t arr_size_mb = atoi(argv[3]);
  int steps = atoi(argv[4]);

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
    // pool_create();

    /** connect to the just created DAOS pool */
    rc = daos_pool_connect(pool_uuid, DSS_PSETID,
                           // DAOS_PC_EX ,
                           DAOS_PC_RW /* read write access */,
                           &poh /* returned pool handle */,
                           NULL /* returned pool info */, NULL /* event */);
    ASSERT(rc == 0, "pool connect failed with %d", rc);
  }

  /** share pool handle with peer tasks */
  handle_share(&poh, HANDLE_POOL, rank, poh, 1);

  if (rank == 0) {
    /** generate uuid for container */
    // uuid_generate(co_uuid);
    /** create container */
    // rc = daos_cont_create(poh, co_uuid, NULL /* properties */,
    //                      NULL /* event */);
    // ASSERT(rc == 0, "container create failed with %d", rc);

    /** open container */
    rc = daos_cont_open(poh, co_uuid, DAOS_COO_RW, &coh, NULL, NULL);
    ASSERT(rc == 0, "container open failed with %d", rc);
  }

  /** share container handle with peer tasks */
  handle_share(&coh, HANDLE_CO, rank, poh, 1);

  /** the other tasks write the array */
  // array(arr_size_mb, steps);
  read_data(procs, arr_size_mb, steps, 0 /* Async I/O flag False*/);

  /** close container */
  daos_cont_close(coh, NULL);

  /** disconnect from pool & destroy it */
  daos_pool_disconnect(poh, NULL);
  // if (rank == 0)
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