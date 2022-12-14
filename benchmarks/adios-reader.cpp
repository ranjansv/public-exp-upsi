#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <vector>

#include <adios2.h>
#include <mpi.h>

#include <caliper/cali-manager.h>
#include <caliper/cali.h>
#include <stdlib.h>

void shuffle(std::vector<int> &array, size_t n) {
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

int main(int argc, char *argv[]) {
  MPI_Init(&argc, &argv);
  std::string engine_type = std::string(argv[1]);
  std::string filename = std::string(argv[2]);
  int num_adios_var = strtol(argv[3], NULL, 10);
  size_t get_size;
  // size_t get_size = strtol(argv[3], NULL, 10);
  // std::string read_pattern = std::string(argv[4]);
  int rank, comm_size, wrank;

  bool flag_random_read;

  std::vector<int> arr_offsets;

  MPI_Comm_rank(MPI_COMM_WORLD, &wrank);

  const unsigned int color = 2;
  MPI_Comm comm;
  MPI_Comm_split(MPI_COMM_WORLD, color, wrank, &comm);

  MPI_Comm_rank(comm, &rank);
  MPI_Comm_size(comm, &comm_size);

  if (rank == 0) {
    std::cout << "engine_type: " << engine_type << std::endl;
    std::cout << "filename: " << filename << std::endl;
    std::cout << "num_adios_var: " << num_adios_var << std::endl;
  }
  // Init ADIOS
  adios2::ADIOS ad("adios2.xml", comm);

  // Open IO instance
  adios2::IO reader_io = ad.DeclareIO(engine_type);

  // Declare variables
  std::vector<std::vector<char> > u;
  int step;
  adios2::Variable<char> *var_u_in;
  adios2::Variable<int> var_step_in;

  var_u_in = new adios2::Variable<char>[num_adios_var];
  u.resize(num_adios_var);

  // Open Engine
  adios2::Engine reader = reader_io.Open(filename, adios2::Mode::Read, comm);

  // Perform Reads
  std::vector<std::size_t> shape;
  /*
    if(read_pattern == "random")
        flag_random_read = true;
    else
        flag_random_read = false;
  */
  flag_random_read = false;

  cali_config_set("CALI_CALIPER_ATTRIBUTE_DEFAULT_SCOPE", "process");
  CALI_MARK_BEGIN("reader:iterations");
  int iter = 1;
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
    char buf[10];
    for (int i = 0; i < num_adios_var; i++) {
      sprintf(buf, "U%d", i + 1);
      var_u_in[i] = reader_io.InquireVariable<char>(buf);
    }
    var_step_in = reader_io.InquireVariable<int>("step");

    // Assumes all ADIOS variables have the same shape
    shape = var_u_in[0].Shape();

    size_t elements_per_adios_var_per_rank = shape[0] / comm_size;
    size_t read_length = elements_per_adios_var_per_rank;
    size_t offset = elements_per_adios_var_per_rank * rank;

    if (!rank) {
      std::cout << "iter: " << iter;
      std::cout << ", elements_per_adios_var_per_rank: "
                << elements_per_adios_var_per_rank << std::endl;
    }

    for (int i = 0; i < num_adios_var; i++) {
      u[i].resize(read_length);
      var_u_in[i]
          .SetSelection(adios2::Box<adios2::Dims>({ offset }, { read_length }));
      reader.Get<char>(var_u_in[i], u[i]);
    }

    /*
    size_t begin_offset = elements_per_adios_var_per_rank * rank;
    size_t offset = begin_offset;

    if (rank == comm_size - 1)
      elements_per_adios_var_per_rank = shape[0] -
    elements_per_adios_var_per_rank * (comm_size - 1);

    if (iter == 1) {
      get_size = elements_per_adios_var_per_rank;
      arr_offsets.resize(elements_per_adios_var_per_rank / get_size);

      for (int i = 0; i < arr_offsets.size(); i++)
        arr_offsets[i] = i;

      if (flag_random_read == true)
        shuffle(arr_offsets, arr_offsets.size());
    }

    // Set selection
    int j = 0;
    while (offset < begin_offset + elements_per_adios_var_per_rank) {
      var_u_in.SetSelection(adios2::Box<adios2::Dims>(
          {begin_offset + arr_offsets[j] * get_size}, {get_size}));
      reader.Get<char>(var_u_in, u);
      offset += get_size;
      j++;
    }*/

    reader.Get<int>(var_step_in, step);

    CALI_MARK_BEGIN("reader:endstep");
    reader.EndStep();
    CALI_MARK_END("reader:endstep");
    CALI_MARK_END("reader:inquire-n-endstep");

    iter++;
  }
  CALI_MARK_END("reader:iterations");

  // cleanup
  reader.Close();

  MPI_Finalize();

  return 0;
}
