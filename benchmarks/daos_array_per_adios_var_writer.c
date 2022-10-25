#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/file.h>
#include <unistd.h>

#include "suite/daos_test.h"
#include <daos.h>
#include <daos/tests_lib.h>
#include <mpi.h>

#include <caliper/cali.h>
#include <caliper/cali-manager.h>

/** local task information */
int rank = -1;
int wrank;
int procs;
char node[128] = "unknown";

enum Pattern {
  Strided = 1,
  Sequence = 0
};

/* MPI communicator for writers */
MPI_Comm comm;

/** Name of the process set associated with the DAOS server */
#define DSS_PSETID "daos_server"
//#define	DSS_PSETID	 "daos_tier0"

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
    req->iov = (d_iov_t) { .iov_buf = data,
                           .iov_buf_len = data_per_rank * sizeof(data[0]),
                           .iov_len = data_per_rank * sizeof(data[0]), };
    req->sg.sg_nr = 1;
    req->sg.sg_iovs = &req->iov;
  }
}

static void array_oh_share(daos_handle_t *oh) {
  d_iov_t ghdl = { NULL, 0, 0 };
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

void write_data(size_t datasize_mb, int put_size, int steps, int async,
                int pattern_flag, int num_adios_var) {
  daos_obj_id_t oid[num_adios_var];
  daos_handle_t oh[num_adios_var];
  daos_array_iod_t iod[num_adios_var];
  daos_range_t *rg[num_adios_var];
  d_sg_list_t sgl[num_adios_var];
  d_iov_t iov;
  char *wbuf = NULL;
  daos_size_t i;
  daos_event_t ev, *evp;
  int rc;
  int iter;
  size_t elements_per_rank;
  daos_size_t size;

  FILE *fp;
  char buf[100];
  int fd;

  daos_size_t cell_size = 1;
  static daos_size_t chunk_size = 1048576;
  uint64_t hi;

  CALI_MARK_BEGIN("daos_array-writer:oid-gen-n-share");
  if (rank == 0) {
    for (i = 0; i < num_adios_var; i++) {
      oid[i].hi = 0;
      oid[i].lo = i;
      daos_array_generate_oid(coh, &oid[i], true, 0, 0, 0);
      rc = daos_array_create(coh, oid[i], DAOS_TX_NONE, cell_size, chunk_size,
                             &oh[i], NULL);
      assert_rc_equal(rc, 0);
      hi = oid[i].hi;
    }

    fp = fopen("./share/oid_hi.txt", "w");
    fprintf(fp, "%lu", hi);
    fclose(fp);

    fp = fopen("./share/oid_part_count.txt", "w");
    fd = fileno(fp);
    if (flock(fd, LOCK_EX) == -1)
      exit(1);
    fprintf(fp, "%d", 1);
    fclose(fp);
  }
  for (i = 0; i < num_adios_var; i++) {
    array_oh_share(&oh[i]);
  }
  CALI_MARK_END("daos_array-writer:oid-gen-n-share");

  elements_per_rank = datasize_mb * MB_in_bytes;
  D_ALLOC_ARRAY(wbuf, elements_per_rank);
  assert_non_null(wbuf);

  for (i = 0; i < elements_per_rank; i++)
    wbuf[i] = i + 1;

  char snapshot_name[50];

  int num_snapshots = 0;
  daos_epoch_t epochs[steps];
  char list_snapnames[steps][50];
  daos_anchor_t anchor;
  memset(&anchor, 0, sizeof(anchor));

  int j;

  CALI_MARK_BEGIN("daos_array-writer:iterations");

  for (iter = 0; iter < steps; iter++) {
    /** set array location */
    for (i = 0; i < num_adios_var; i++) {
      uint64_t elements_per_adios_var_per_rank =
          elements_per_rank / num_adios_var;
      iod[i].arr_nr = elements_per_adios_var_per_rank / put_size;
      rg[i] = (daos_range_t *)malloc(iod[i].arr_nr * sizeof(daos_range_t));
      daos_off_t start_index = rank * elements_per_rank / sizeof(char);
      daos_size_t write_length = sizeof(char) * put_size;

      int arr_offsets[iod[i].arr_nr];

      for (j = 0; j < iod[i].arr_nr; j++) {
        if (pattern_flag == Sequence)
          arr_offsets[j] = j;
        else
          arr_offsets[j] = (j + 14) % iod[i].arr_nr;

        rg[i][j].rg_len = write_length;
        rg[i][j].rg_idx = start_index + arr_offsets[j] * put_size;
      }
      iod[i].arr_rgs = rg[i];

      /** set memory location */
      sgl[i].sg_nr = 1;
      d_iov_set(&iov, wbuf, elements_per_adios_var_per_rank * sizeof(char));
      sgl[i].sg_iovs = &iov;
    }

    CALI_MARK_BEGIN("daos_array-writer:write-time");
    for (i = 0; i < num_adios_var; i++) {
      rc = daos_array_write(oh[i], DAOS_TX_NONE, &iod[i], &sgl[i],
                            async ? &ev : NULL);
      assert_rc_equal(rc, 0);
    }
    CALI_MARK_END("daos_array-writer:write-time");

    MPI_Barrier(MPI_COMM_WORLD);
    for (i = 0; i < num_adios_var; i++)
      free(rg[i]);

    CALI_MARK_BEGIN("daos_array-writer:snapshot-time");
    if (rank == 0) {
      rc = daos_cont_create_snap(coh, &epoch, NULL, NULL);

      printf("daos_cont_create_snap, rc = %d\n", rc);
      printf("epoch = %lu\n", epoch);
      ASSERT(rc == 0, "daos_cont_create_snap failed with %d", rc);

      sprintf(buf, "./share/container-snap-%d.txt", iter);
      fp = fopen(buf, "w");
      fprintf(fp, "%lu", epoch);
      fclose(fp);

      fp = fopen("./share/snapshot_count.txt", "w");
      fd = fileno(fp);
      if (flock(fd, LOCK_EX) == -1)
        exit(1);
      fprintf(fp, "%d", iter + 1);
      fclose(fp);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    CALI_MARK_END("daos_array-writer:snapshot-time");
  }
  CALI_MARK_END("daos_array-writer:iterations");

  if (rank == 0) {
    for (i = 0; i < num_adios_var; i++) {
      rc = daos_array_get_size(oh[i], DAOS_TX_NONE, &size, NULL);
      ASSERT(rc == 0, "daos_array_get_size failed with %d", rc);
      printf("Array %d size = %lu\n", i + 1, size);
    }

    rc = daos_cont_list_snap(coh, &num_snapshots, epochs, list_snapnames,
                             &anchor, NULL);
    ASSERT(rc == 0, "daos_cont_list_snap failed with %d", rc);

    printf("Number of snapshots: %d\n", num_snapshots);
  }
  D_FREE(wbuf);
}

int main(int argc, char **argv) {
  int rc;
  uuid_parse(argv[1], pool_uuid);
  uuid_parse(argv[2], co_uuid);
  size_t datasize_mb = strtol(argv[3], NULL, 10);
  int put_size = strtol(argv[4], NULL, 10);
  int steps = strtol(argv[5], NULL, 10);
  // char *pattern = argv[6];
  int num_adios_var = strtol(argv[6], NULL, 10);
  int pattern_flag = 0;

  /*
    if (strcmp(pattern, "sequential") == 0)
      pattern_flag = 0;
    else
      pattern_flag = 1;
  */
  pattern_flag = Sequence;

  rc = gethostname(node, sizeof(node));
  ASSERT(rc == 0, "buffer for hostname too small");

  cali_config_set("CALI_CALIPER_ATTRIBUTE_DEFAULT_SCOPE", "process");

  rc = MPI_Init(&argc, &argv);
  ASSERT(rc == MPI_SUCCESS, "MPI_Init failed with %d", rc);

  MPI_Comm_size(MPI_COMM_WORLD, &wrank);

  const unsigned int color = 1;
  MPI_Comm_split(MPI_COMM_WORLD, color, wrank, &comm);

  MPI_Comm_rank(comm, &rank);
  MPI_Comm_size(comm, &procs);

  if (rank == 0) {
    printf("datasize_mb = %lu\n", datasize_mb);
    printf("put_size = %d\n", put_size);
    printf("steps = %d\n", steps);
    printf("num_adios_var = %d\n", num_adios_var);
  }

  /** initialize the local DAOS stack */
  rc = daos_init();
  ASSERT(rc == 0, "daos_init failed with %d", rc);

  /** create event queue */
  rc = daos_eq_create(&eq);
  ASSERT(rc == 0, "eq create failed with %d", rc);

  CALI_MARK_BEGIN("daos_array-writer:pool_connect");
  if (rank == 0) {
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
  CALI_MARK_END("daos_array-writer:pool_connect");

  CALI_MARK_BEGIN("daos_array-writer:cont_connect");
  if (rank == 0) {
    /** open container */
    rc = daos_cont_open(poh, co_uuid, DAOS_COO_RW, &coh, NULL, NULL);
    ASSERT(rc == 0, "container open failed with %d", rc);
  }

  /** share container handle with peer tasks */
  handle_share(&coh, HANDLE_CO, rank, poh, 1);
  CALI_MARK_END("daos_array-writer:cont_connect");

  /** the other tasks write the array */
  write_data(datasize_mb, put_size, steps, 0 /* Async I/O flag False*/,
             pattern_flag, num_adios_var);

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
