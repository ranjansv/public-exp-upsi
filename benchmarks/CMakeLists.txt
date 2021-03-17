cmake_minimum_required(VERSION 3.12)
project(benchmark C CXX)

find_package(MPI REQUIRED)
find_package(ADIOS2 REQUIRED)
#...
add_executable(hello hello-world.cpp)
add_executable(hello-hl hello-world-hl.cpp)
target_link_libraries(hello PRIVATE adios2::cxx11_mpi MPI::MPI_C)
target_link_libraries(hello-hl PRIVATE adios2::cxx11_mpi MPI::MPI_C)
