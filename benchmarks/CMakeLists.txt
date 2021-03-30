cmake_minimum_required(VERSION 3.12)
project(benchmark C CXX)

find_package(MPI REQUIRED)
find_package(ADIOS2 REQUIRED)
#...
add_executable(writer writer.cpp)
target_link_libraries(writer PRIVATE adios2::cxx11_mpi MPI::MPI_C)

add_executable(reader reader.cpp)
target_link_libraries(reader PRIVATE adios2::cxx11_mpi MPI::MPI_C)

set(CMAKE_INSTALL_BINDIR /home/ranjan/next-project/exp-upsi/benchmarks/bin)

install(TARGETS writer
	RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}) 
