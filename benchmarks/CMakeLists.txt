cmake_minimum_required(VERSION 3.12)
project(benchmark C CXX)

#set(CMAKE_CXX_FLAGS "-fpic -g -std=c++11 -Wno-write-strings") 
#set(CMAKE_CXX_FLAGS "-D_GLIBCXX_USE_CXX11_ABI=0 -std=c++11")
#set(CMAKE_CXX_FLAGS "-g -std=c++11") 



find_package(MPI REQUIRED)
find_package(ADIOS2 REQUIRED)

#add_compile_definitions(-D_GLIBCXX_USE_CXX11_ABI=0)

include_directories(/home/ranjan/next-project/daos/src/include)
include_directories(/home/ranjan/next-project/daos/install/include/)
include_directories(/home/ranjan/next-project/daos/install/prereq/dev/pmdk/include/)
include_directories(/home/ranjan/next-project/daos/install/prereq/dev/argobots/include/)
include_directories(/home/ranjan/next-project/daos/src/tests)
link_directories(/home/ranjan/next-project/daos/install/lib64)

add_executable(writer writer.cpp)
target_link_libraries(writer PRIVATE adios2::cxx11_mpi MPI::MPI_C )

add_executable(reader reader.cpp)
target_link_libraries(reader PRIVATE adios2::cxx11_mpi MPI::MPI_C)

add_executable(daos-writer daos-writer.c)
target_link_libraries(daos-writer PRIVATE daos daos_common gurt cart  uuid cmocka daos_tests MPI::MPI_C)


#add_executable(simple_array simple_array.c)
#target_link_libraries(simple_array PRIVATE daos daos_common gurt cart  uuid cmocka daos_tests MPI::MPI_C)

#add_executable(daos-posix-writer daos-posix-writer.c)
#target_link_libraries(daos-posix-writer PRIVATE daos daos_common dfs gurt cart  uuid cmocka daos_tests MPI::MPI_C)
#
#add_executable(daos-posix-reader daos-posix-reader.c)
#target_link_libraries(daos-posix-reader PRIVATE daos daos_common dfs gurt cart  uuid cmocka daos_tests MPI::MPI_C)
