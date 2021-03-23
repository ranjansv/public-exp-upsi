#include <iostream>
#include <stdexcept>
#include <vector>

#include <adios2.h>
#include <mpi.h>

#include "writer.h"

#define GB_2_bytes 1073741824

Writer::Writer(adios2::IO io, size_t arr_size_gb)
: io(io)
{

    int procs;
    int rank;

    MPI_Comm_size(MPI_COMM_WORLD, &procs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    global_array_size = arr_size_gb * GB_2_bytes/sizeof(double);
    local_size = global_array_size/procs;
    offset = rank * local_size;
    
    var_array = io.DefineVariable<double>("U",{global_array_size},
		    {offset}, {local_size});

    var_step = io.DefineVariable<int>("step");

}

void Writer::open(const std::string &fname) {
    writer = io.Open(fname, adios2::Mode::Write);
}

void Writer::write(int step)
{
	std::vector<double> u(local_size,(double)step);

	writer.BeginStep();
	writer.Put<int>(var_step, &step);
	writer.Put<double>(var_array, u.data());
	writer.EndStep();
}

void Writer::close() { writer.Close(); }

int main(int argc, char *argv[])
{

    MPI_Init(&argc, &argv);
    std::string config_file = std::string(argv[1]);
    size_t arr_size_gb = std::stoi(argv[2]);
    int steps = std::stoi(argv[3]);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if(rank == 0) {
    std::cout << "adios config file: " << config_file << std::endl;
    std::cout << "arr_size_gb: " << arr_size_gb << std::endl;
    std::cout << "steps: " << steps << std::endl;
    }
    try
    {
        adios2::ADIOS adios(config_file,MPI_COMM_WORLD);
        adios2::IO io = adios.DeclareIO("Writers");
	Writer writer_obj(io, arr_size_gb);

	writer_obj.open("/mnt/pmem1/output.bp");

	for(int i = 0; i < steps; i++) {
		writer_obj.write(steps);
	}

	writer_obj.close();


    }
    catch (std::exception &e)
    {
        std::cout << "ERROR: ADIOS2 exception: " << e.what() << "\n";
        MPI_Abort(MPI_COMM_WORLD, -1);
    }

    MPI_Finalize();
    return 0;
}
