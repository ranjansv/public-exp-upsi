#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <vector>

#include <adios2.h>
#include <mpi.h>

#include "writer.h"
#include "timer.hpp"

#define GB_in_bytes 1073741824

#define ENABLE_TIMERS

Writer::Writer(adios2::IO io, int rank, int procs, size_t arr_size_gb)
    : io(io) {

  global_array_size = arr_size_gb * GB_in_bytes / sizeof(double);
  local_size = global_array_size / procs;
  offset = rank * local_size;

  var_array = io.DefineVariable<double>("U", {global_array_size}, {offset},
                                        {local_size});

  var_step = io.DefineVariable<int>("step");
}

void Writer::open(const std::string &fname) {
  writer = io.Open(fname, adios2::Mode::Write);
}

void Writer::write(int step) {
  std::vector<double> u(local_size, (double)step);

  writer.BeginStep();
  writer.Put<int>(var_step, &step);
  writer.Put<double>(var_array, u.data());
  writer.EndStep();
}

void Writer::close() { writer.Close(); }

int main(int argc, char *argv[]) {

  MPI_Init(&argc, &argv);
  std::string config_file = std::string(argv[1]);
  size_t arr_size_gb = std::stoi(argv[2]);
  int steps = std::stoi(argv[3]);

  int rank, procs, wrank;
  MPI_Comm_rank(MPI_COMM_WORLD, &wrank);

  const unsigned int color = 1;
  MPI_Comm comm;
  MPI_Comm_split(MPI_COMM_WORLD, color, wrank, &comm);

  MPI_Comm_rank(comm, &rank);
  MPI_Comm_size(comm, &procs);

  if (rank == 0) {
    std::cout << "adios config file: " << config_file << std::endl;
    std::cout << "arr_size_gb: " << arr_size_gb << std::endl;
    std::cout << "steps: " << steps << std::endl;
  }
  try {
    adios2::ADIOS adios(config_file, MPI_COMM_WORLD);
    adios2::IO io = adios.DeclareIO("Writers");
    Writer writer_obj(io, rank, procs, arr_size_gb);

    writer_obj.open("/mnt/pmem1/output.bp");
#ifdef ENABLE_TIMERS
    Timer timer_total;
    Timer timer_compute;
    Timer timer_write;

    std::ostringstream log_fname;
    log_fname << "writer-" << rank << ".log";

    std::ofstream log(log_fname.str());
    log << "step\ttotal\tcompute\twrite" << std::endl;
#endif

    for (int i = 0; i < steps; i++) {
#ifdef ENABLE_TIMERS
      MPI_Barrier(comm);
      timer_total.start();
      timer_compute.start();
#endif

      /*Compute kernel can be
      added here if required*/

#ifdef ENABLE_TIMERS
      double time_compute = timer_compute.stop();
      MPI_Barrier(comm);
      timer_write.start();
#endif

      writer_obj.write(steps);

#ifdef ENABLE_TIMERS
      double time_write = timer_write.stop();
      double time_step = timer_total.stop();
      MPI_Barrier(comm);

      log << i << "\t" << time_step << "\t" << time_compute << "\t"
          << time_write << std::endl;
#endif
    }

    writer_obj.close();

#ifdef ENABLE_TIMERS
    log << "total\t" << timer_total.elapsed() << "\t" << timer_compute.elapsed()
        << "\t" << timer_write.elapsed() << std::endl;

    log.close();
#endif

  } catch (std::exception &e) {
    std::cout << "ERROR: ADIOS2 exception: " << e.what() << "\n";
    MPI_Abort(MPI_COMM_WORLD, -1);
  }

  MPI_Finalize();
  return 0;
}
