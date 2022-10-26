#include <adios2.h>

class Writer {
  adios2::IO io;
  adios2::Engine writer;
  adios2::Variable<char> *var_array;
  adios2::Variable<int> var_step;

  size_t global_array_size;
  size_t offset;
  size_t elements_per_rank;
  size_t elements_per_adios_var_per_rank;
  size_t put_size;
  size_t num_blocks_per_adios_var;

  int num_adios_var;
  std::vector<std::vector<char>> u;

  int my_rank;

public:
  Writer(adios2::IO, int, int, size_t, size_t, int);
  void open(const std::string &fname);
  void write(int step);
  void close();
};
