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
  Random = 1,
  Sequential = 0
};

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
    req->iov = (d_iov_t) { .iov_buf = data,
                           .iov_buf_len = data_per_rank * sizeof(data[0]),
                           .iov_len = data_per_rank * sizeof(data[0]), };
    req->sg.sg_nr = 1;
    req->sg.sg_iovs = &req->iov;
  }
}

void array(size_t datasize_mb, int steps) {
  daos_handle_t oh;
  struct io_req *reqs;
  int rc;
  int iter;

  size_t data_per_rank = datasize_mb * MB_in_bytes;

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

void shuffle(int *array, size_t n) {
  if (n > 1) {
    size_t i;
    for (i = 0; i < n - 1; i++) {
      size_t j = i + rand() / (RAND_MAX / (n - i) + 1);
      int t = array[j];
      array[j] = array[i];
      array[i] = t;
    }
  }
}

void read_data(size_t datasize_mb, size_t get_size, int steps, int async,
               int flag_read_pattern, int read_ratio, int put_size,
               int num_adios_var) {
  daos_handle_t th;
  char *rbuf[num_adios_var];
  int rc;
  int iter;
  int i, j, k;
  size_t elements_per_rank;
  size_t elements_per_adios_var;

  FILE *fp;
  char buf[100];

  /* Temporary assignment */
  daos_size_t cell_size = 1;
  static daos_size_t chunk_size = MB_in_bytes;

  daos_obj_id_t oids[read_ratio], oid;
  daos_handle_t oh[read_ratio];
  daos_range_t *rg[num_adios_var][read_ratio];
  daos_array_iod_t iod[num_adios_var][read_ratio];
  d_sg_list_t sgl[num_adios_var][read_ratio];
  d_iov_t iov;
  uint32_t oids_nr;
  uint64_t hi;
  daos_anchor_t anchor;

  int num_snapshots;
  char list_snapnames[steps][50];
  daos_epoch_t epochs[steps];
  int fd;

  int num_committed_snapshots = 0;
  int oid_part_count = 0;

  // Rank 0 read the OID hi value which is common across all writer ranks and
  // share it
  if (rank == 0) {
    printf("datasize_mb = %d\n", datasize_mb);

    while (oid_part_count != 1) {
      usleep(10000);
      fp = fopen("./share/oid_part_count.txt", "r");
      fd = fileno(fp);
      if (flock(fd, LOCK_EX) == -1)
        exit(1);
      fscanf(fp, "%d", &oid_part_count);
      fclose(fp);
    }

    fp = fopen("share/oid_hi.txt", "r");
    fscanf(fp, "%lu", &hi);
    fclose(fp);
    printf("rank = 0, hi = %lu\n", hi);
  }
  rc = MPI_Bcast(&hi, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);
  assert_int_equal(rc, MPI_SUCCESS);

  // Instantiate Object IDs of the required writer ranks
  oids_nr = read_ratio;
  for (iter = 0; iter < oids_nr; iter++) {
    // oid.lo is set to the rank ID of the writer
    oids[iter].lo = rank * read_ratio + iter;
    oids[iter].hi = hi;
  }

  // Allocate memory for receive buffer
  elements_per_rank = datasize_mb * MB_in_bytes * read_ratio;
  elements_per_adios_var = elements_per_rank / num_adios_var;
  for (i = 0; i < num_adios_var; i++) {
    D_ALLOC_ARRAY(rbuf[i], elements_per_adios_var);
    assert_non_null(rbuf[i]);
  }

  get_size = elements_per_rank;

  CALI_MARK_BEGIN("daos_array-reader:iterations");

  for (iter = 0; iter < steps; iter++) {

    CALI_MARK_BEGIN("daos_array-reader:get_epochid");
    if (rank == 0) {
      printf("Waiting to read epoch of snapshot %d\n", iter + 1);

      while (iter + 1 > num_committed_snapshots) {
        usleep(10000);
        fp = fopen("./share/snapshot_count.txt", "r");
        fd = fileno(fp);
        if (flock(fd, LOCK_EX) == -1)
          exit(1);
        fscanf(fp, "%lu", &num_committed_snapshots);
        fclose(fp);
      }

      sprintf(buf, "share/container-snap-%d.txt", iter);
      fp = fopen(buf, "r");
      fscanf(fp, "%lu", &epochs[iter]);
      fclose(fp);
    }
    // MPI share epoch
    rc = MPI_Bcast(&epochs[iter], 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);
    assert_int_equal(rc, MPI_SUCCESS);
    CALI_MARK_END("daos_array-reader:get_epochid");

    printf("iter %d rank %d epoch: %lu\n", iter + 1, rank, epochs[iter]);

    CALI_MARK_BEGIN("daos_array-reader:open_snap");
    rc = daos_tx_open_snap(coh, epochs[iter], &th, NULL);
    ASSERT(rc == 0, "daos_tx_open_snap failed with %d", rc);
    CALI_MARK_END("daos_array-reader:open_snap");

    CALI_MARK_BEGIN("daos_array-reader:open_array");
    for (i = 0; i < oids_nr; i++) {
      rc = daos_array_open(coh, oids[i], th, DAOS_OO_RW, &cell_size,
                           &chunk_size, &oh[i], NULL);
      ASSERT(rc == 0, "daos_array_open failed with %d", rc);
    }
    CALI_MARK_END("daos_array-reader:open_array");

    for (i = 0; i < num_adios_var; i++) {
      uint64_t elements_from_each_writer_per_var =
          elements_per_adios_var / read_ratio;
      size_t num_rg_per_writer = elements_from_each_writer_per_var / put_size;

      // Parse all objects for this ADIOS var
      for (j = 0; j < oids_nr; j++) {
        daos_off_t read_index = i * put_size;
        daos_size_t read_length = put_size;
        iod[i][j].arr_nr = num_rg_per_writer;
        rg[i][j] =
            (daos_range_t *)malloc(num_rg_per_writer * sizeof(daos_range_t));
        for (k = 0; k < num_rg_per_writer; k++) {
          rg[i][j][k].rg_len = read_length;
          rg[i][j][k].rg_idx = read_index;
          read_index = read_index + num_adios_var * put_size;
        }
        iod[i][j].arr_rgs = rg[i][j];

        /** set memory location */
        sgl[i][j].sg_nr = 1;
        d_iov_set(&iov, rbuf[i],
                  elements_from_each_writer_per_var * sizeof(char));
        sgl[i][j].sg_iovs = &iov;
      }
    }

    CALI_MARK_BEGIN("daos_array-reader:read-time");
    for (i = 0; i < num_adios_var; i++) {
      for (j = 0; j < oids_nr; j++) {
        rc = daos_array_read(oh[j], th, &iod[i][j], &sgl[i][j], NULL);
        ASSERT(rc == 0, "daos_array_read failed with %d", rc);
      }
    }
    CALI_MARK_END("daos_array-reader:read-time");

    for (i = 0; i < num_adios_var; i++) {
      for (j = 0; j < oids_nr; j++) {
        free(rg[i][j]);
      }
    }

    CALI_MARK_BEGIN("daos_array-reader:close_array");
    for (i = 0; i < oids_nr; i++) {
      rc = daos_array_close(oh[i], NULL);
      ASSERT(rc == 0, "daos_array_close failed with %d", rc);
    }
    CALI_MARK_END("daos_array-reader:close_array");

    MPI_Barrier(MPI_COMM_WORLD);
  }
  CALI_MARK_END("daos_array-reader:iterations");

  for (i = 0; i < num_adios_var; i++)
    D_FREE(rbuf[i]);
}

int main(int argc, char **argv) {
  int rc;
  uuid_parse(argv[1], pool_uuid);
  uuid_parse(argv[2], co_uuid);
  size_t datasize_mb = strtol(argv[3], NULL, 10);
  int steps = strtol(argv[4], NULL, 10);
  int read_ratio = strtol(argv[5], NULL, 10);
  int put_size = strtol(argv[6], NULL, 10);
  int num_adios_var = strtol(argv[7], NULL, 10);
  int flag_read_pattern;

  // get_size is assigned a dummy value, all writer data will be read at once
  int get_size = 0;
  /*
    if (strcmp(read_pattern, "random") == 0)
      flag_read_pattern = Random;
    else
      flag_read_pattern = Sequential;
  */
  flag_read_pattern = Sequential;

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
    printf("steps = %d\n", steps);
    printf("read_ratio = %d\n", read_ratio);
    printf("put_size = %d\n", put_size);
    printf("num_adios_var = %d\n", num_adios_var);
  }

  /** initialize the local DAOS stack */
  rc = daos_init();
  ASSERT(rc == 0, "daos_init failed with %d", rc);

  /** create event queue */
  rc = daos_eq_create(&eq);
  ASSERT(rc == 0, "eq create failed with %d", rc);

  CALI_MARK_BEGIN("daos_array-reader:pool_connect");
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
  CALI_MARK_END("daos_array-reader:pool_connect");

  CALI_MARK_BEGIN("daos_array-reader:cont_connect");
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
  CALI_MARK_END("daos_array-reader:cont_connect");

  /** the other tasks write the array */
  // array(datasize_mb, steps);
  read_data(datasize_mb, get_size, steps, 0 /* Async I/O flag False*/,
            flag_read_pattern, read_ratio, put_size, num_adios_var);

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
