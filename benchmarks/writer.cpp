#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <vector>

#include <adios2.h>
#include <mpi.h>

#include <caliper/cali.h>
#include <caliper/cali-manager.h>

#include "timer.hpp"
#include "writer.h"

#define MB_in_bytes 1048576

#define ENABLE_TIMERS
#undef ENABLE_TIMERS

Writer::Writer(adios2::IO io, int rank, int procs, size_t arr_size_mb)
    : io(io) {

  global_array_size = arr_size_mb * MB_in_bytes / sizeof(char);
  local_size = global_array_size / procs;
  offset = rank * local_size;

  var_array = io.DefineVariable<char>("U", { global_array_size }, { offset },
                                        { local_size });

  var_step = io.DefineVariable<int>("step");
}

void Writer::open(const std::string &fname) {
  writer = io.Open(fname, adios2::Mode::Write);
}

int Writer::getlocalsize() { return local_size; }

void Writer::write(int step, std::vector<char> &u) {

  writer.BeginStep();
  writer.Put<int>(var_step, &step);
  writer.Put<char>(var_array, u.data());
  writer.EndStep();
}

void Writer::close() { writer.Close(); }

int main(int argc, char *argv[]) {

  MPI_Init(&argc, &argv);
  std::string engine_type = std::string(argv[1]);
  std::string filename = std::string(argv[2]);
  size_t arr_size_mb = std::stoi(argv[3]);
  int steps = std::stoi(argv[4]);

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
    std::cout << "arr_size_mb: " << arr_size_mb << std::endl;
    std::cout << "steps: " << steps << std::endl;
  }
  try {
    adios2::ADIOS adios("./adios2.xml", comm);
    adios2::IO io_handle = adios.DeclareIO(engine_type);
    Writer writer(io_handle, rank, procs, arr_size_mb);

    int localsize = writer.getlocalsize();

    writer.open(filename);

    std::vector<char> u(localsize, 0);

    cali_config_set("CALI_CALIPER_ATTRIBUTE_DEFAULT_SCOPE", "process");

    CALI_MARK_BEGIN("writer:iterations");

    for (int i = 0; i < steps; i++) {
      /*Compute kernel can be
      added here if required*/
      CALI_MARK_BEGIN("writer:write-time-outside-barrier");
      MPI_Barrier(MPI_COMM_WORLD);
      CALI_MARK_BEGIN("writer:write-time-inside-barrier");
      writer.write(i + 1, u);
      CALI_MARK_END("writer:write-time-inside-barrier");
      MPI_Barrier(MPI_COMM_WORLD);
      CALI_MARK_END("writer:write-time-outside-barrier");
      if(rank == 0)
         std::cout << "Step = " << i + 1 << std::endl;
    }
    writer.close();
    CALI_MARK_END("writer:iterations");
  }
  catch (std::exception &e) {
    std::cout << "ERROR: ADIOS2 exception: " << e.what() << "\n";
    MPI_Abort(MPI_COMM_WORLD, -1);
  }

  MPI_Finalize();
  return 0;
}
