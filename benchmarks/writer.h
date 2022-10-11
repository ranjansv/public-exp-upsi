#include <adios2.h>

class Writer {
  adios2::IO io;
  adios2::Engine writer;
  adios2::Variable<char> var_array;
  adios2::Variable<int> var_step;

  size_t global_array_size;
  size_t offset;
  size_t local_size;
  size_t block_size;
  size_t num_blocks;

  int my_rank;

public:
  Writer(adios2::IO, int, int, size_t);
  int getlocalsize();
  void open(const std::string &fname);
  void write(int step, std::vector<char>&);
  void close();
};
