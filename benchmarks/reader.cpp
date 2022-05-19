#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <vector>

#include <adios2.h>
#include <mpi.h>

#include "timer.hpp"

#include <caliper/cali.h>
#include <caliper/cali-manager.h>

int main(int argc, char *argv[]) {
  MPI_Init(&argc, &argv);
  std::string engine_type = std::string(argv[1]);
  std::string filename = std::string(argv[2]);
  size_t iosize_bytes = std::stoi(argv[3]);
  int rank, comm_size, wrank;

  MPI_Comm_rank(MPI_COMM_WORLD, &wrank);

  const unsigned int color = 2;
  MPI_Comm comm;
  MPI_Comm_split(MPI_COMM_WORLD, color, wrank, &comm);

  MPI_Comm_rank(comm, &rank);
  MPI_Comm_size(comm, &comm_size);
  // Init ADIOS
  adios2::ADIOS ad("adios2.xml", comm);

  // Open IO instance
  adios2::IO reader_io = ad.DeclareIO(engine_type);

  // Declare variables
  std::vector<double> u;
  int step;
  adios2::Variable<double> var_u_in;
  adios2::Variable<int> var_step_in;

  size_t count = iosize_bytes/sizeof(double);

  // Open Engine
  adios2::Engine reader = reader_io.Open(filename, adios2::Mode::Read, comm);

  // Perform Reads
  std::vector<std::size_t> shape;

  cali_config_set("CALI_CALIPER_ATTRIBUTE_DEFAULT_SCOPE", "process");
  CALI_MARK_BEGIN("reader:loop");

  while (true) {

    // Begin step
    CALI_MARK_BEGIN("reader:beginstep");
    adios2::StepStatus read_status = reader.BeginStep(adios2::StepMode::Read);
    CALI_MARK_END("reader:beginstep");

    CALI_MARK_BEGIN("reader:inquire-n-endstep");
    if (read_status == adios2::StepStatus::NotReady) {
      // std::cout << "Stream not ready yet. Waiting...\n";
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
      continue;
    } else if (read_status != adios2::StepStatus::OK) {
      CALI_MARK_END("reader:inquire-n-endstep");
      break;
    }

    // Inquire variable and set the selection at the first step only
    // This assumes that the variable dimensions do not change across
    // timesteps

    // Inquire variable
    var_u_in = reader_io.InquireVariable<double>("U");
    var_step_in = reader_io.InquireVariable<int>("step");

    shape = var_u_in.Shape();

    size_t total_readcount = shape[0] / comm_size;
    size_t begin_offset = total_readcount * rank;
    size_t offset = begin_offset;

    if (rank == comm_size - 1)
      total_readcount = shape[0] - total_readcount * (comm_size - 1);

    // Set selection
    while ( offset < begin_offset + total_readcount ) {
        var_u_in.SetSelection(adios2::Box<adios2::Dims>({ offset }, { count }));
        reader.Get<double>(var_u_in, u);
        offset += count;
    }

    reader.Get<int>(var_step_in, step);

    CALI_MARK_BEGIN("reader:endstep");
    reader.EndStep();
    CALI_MARK_END("reader:endstep");
    CALI_MARK_END("reader:inquire-n-endstep");
  }
  CALI_MARK_END("reader:loop");

  // cleanup
  reader.Close();

  MPI_Finalize();

  return 0;
}
