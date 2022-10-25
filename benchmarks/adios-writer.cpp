#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <vector>

#include <adios2.h>
#include <mpi.h>

#include <caliper/cali-manager.h>
#include <caliper/cali.h>

#include "adios-writer.h"

#define MB_in_bytes 1048576

#define ENABLE_TIMERS
#undef ENABLE_TIMERS

Writer::Writer(adios2::IO io, int rank, int procs, size_t datasize_mb,
               int put_size, int num_adios_var)
    : io(io), num_adios_var(num_adios_var), put_size(put_size) {

  elements_per_rank = datasize_mb * MB_in_bytes / sizeof(char);
  global_array_size = elements_per_rank * procs;
  elements_per_adios_var_per_rank = elements_per_rank / num_adios_var;

  num_blocks_per_adios_var = elements_per_adios_var_per_rank / put_size;

  var_array = new adios2::Variable<char>[ num_adios_var ];

  char buf[10];

  u.resize(num_adios_var);
  for (int i = 0; i < num_adios_var; i++) {

    sprintf(buf, "U%d", i + 1);
    var_array[i] = io.DefineVariable<char>(buf, {global_array_size},
                                           adios2::Dims(), adios2::Dims());
   u[i].resize(elements_per_adios_var_per_rank);
  }
  var_step = io.DefineVariable<int>("step");

  my_rank = rank;
}

void Writer::open(const std::string &fname) {
  writer = io.Open(fname, adios2::Mode::Write);
}


void Writer::write(int step) {

  writer.BeginStep();
  writer.Put<int>(var_step, &step);
  size_t curr_offset = my_rank * elements_per_adios_var_per_rank;
  for (int i = 0; i < num_blocks_per_adios_var; i++) {
    for (int j = 0; j < num_adios_var; j++) {
      var_array[j].SetSelection(
          adios2::Box<adios2::Dims>({curr_offset}, {put_size}));
      writer.Put<char>(var_array[j], u[j].data());
    }
    curr_offset = curr_offset + put_size;
  }
  writer.EndStep();
}

void Writer::close() { writer.Close(); }

int main(int argc, char *argv[]) {

  MPI_Init(&argc, &argv);
  std::string engine_type = std::string(argv[1]);
  std::string filename = std::string(argv[2]);
  size_t datasize_mb = strtol(argv[3], NULL, 10);
  int steps = strtol(argv[4], NULL, 10);
  int put_size = strtol(argv[5], NULL, 10);
  int num_adios_var = strtol(argv[6], NULL, 10);

  int rank, procs, wrank;
  MPI_Comm_rank(MPI_COMM_WORLD, &wrank);

  const unsigned int color = 1;
  MPI_Comm comm;
  MPI_Comm_split(MPI_COMM_WORLD, color, wrank, &comm);

  MPI_Comm_rank(comm, &rank);
  MPI_Comm_size(comm, &procs);

  if (rank == 0) {
    std::cout << "engine_type: " << engine_type << std::endl;
    std::cout << "filename: " << filename << std::endl;
    std::cout << "datasize_mb: " << datasize_mb << std::endl;
    std::cout << "steps: " << steps << std::endl;
    std::cout << "num_adios_var: " << num_adios_var << std::endl;
  }
  try {
    adios2::ADIOS adios("./adios2.xml", comm);
    adios2::IO io_handle = adios.DeclareIO(engine_type);
    Writer writer(io_handle, rank, procs, datasize_mb, put_size);


    writer.open(filename);


    cali_config_set("CALI_CALIPER_ATTRIBUTE_DEFAULT_SCOPE", "process");

    CALI_MARK_BEGIN("writer:iterations");

    for (int i = 0; i < steps; i++) {
      /*Compute kernel can be
      added here if required*/
      CALI_MARK_BEGIN("writer:write-time");
      writer.write(i + 1);
      CALI_MARK_END("writer:write-time");
      MPI_Barrier(MPI_COMM_WORLD);
      if (rank == 0)
        std::cout << "Step = " << i + 1 << std::endl;
    }
    writer.close();
    CALI_MARK_END("writer:iterations");
  } catch (std::exception &e) {
    std::cout << "ERROR: ADIOS2 exception: " << e.what() << "\n";
    MPI_Abort(MPI_COMM_WORLD, -1);
  }

  MPI_Finalize();
  return 0;
}
