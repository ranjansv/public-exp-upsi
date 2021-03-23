#include <adios2.h>

class Writer {
  adios2::IO io;
  adios2::Engine writer;
  adios2::Variable<double> var_array;
  adios2::Variable<int> var_step;

  size_t global_array_size;
  size_t offset;
  size_t local_size;

public:
  Writer(adios2::IO, int, int, size_t);
  void open(const std::string &fname);
  void write(int step);
  void close();
};
