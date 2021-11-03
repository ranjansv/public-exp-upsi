#!/bin/bash
#SBATCH -J upsi-bench           # Job name
#SBATCH -o upsi-bench.o%j       # Name of stdout output file
#SBATCH -e upsi-bench.e%j       # Name of stderr error file
#SBATCH -p flex			# Queue (partition) name
#SBATCH -N 7                # Total # of nodes 
#SBATCH -n 196              # Total # of mpi tasks
#SBATCH --ntasks-per-node=28
#SBATCH -t 00:45:00        # Run time (hh:mm:ss)
#SBATCH --mail-type=all    # Send email at begin and end of job
#SBATCH --mail-user=ranjansv@gmail.com

echo "POOL_UUID: $POOL_UUID"
echo "SLURM_JOB_NUM_NODES: $SLURM_JOB_NUM_NODES"

CONFIG_FILE=$1

#Load config
source ${CONFIG_FILE}

#Create timestamped output directory
TIMESTAMP=`echo $(date +%Y-%m-%d-%H:%M:%S)`
RESULT_DIR="results/$TIMESTAMP"
mkdir -p $RESULT_DIR

rm results/latest
ln -s $TIMESTAMP results/latest

#mount > $RESULT_DIR/fs-mounts.log
git branch --show-current > $RESULT_DIR/git.log 
git log --format="%H" -n 1 >> $RESULT_DIR/git.log

#Build source
cd build
make clean && make
cd ..

#Copy configs and xml to outputdir
cp ${CONFIG_FILE} $RESULT_DIR/config.sh
cp ./adios2.xml $RESULT_DIR

SCRIPT_NAME="bench-run.sh"
cp ./$SCRIPT_NAME  $RESULT_DIR/



is_daos_agent_running=`pgrep daos_agent`
echo $is_daos_agent_running
if [[ $is_daos_agent_running -eq "" ]]
then
export TACC_TASKS_PER_NODE=1
ibrun -np $SLURM_JOB_NUM_NODES  ~/bin/daos_startup.sh
unset TACC_TASKS_PER_NODE
else
   echo "daos_agent is already running"
fi

RANKS_PER_NODE=28

for NR in $PROCS
do
    for IO_NAME in $ENGINE
    do
	#Parse IO_NAME for engine and storage type in case of DAOS
	if grep -q "daos-posix" <<< "$IO_NAME"; then
		ENG_TYPE="daos-posix"
		FILENAME="./mnt/dfuse/output.bp"
		MOUNTPOINT="/work2/08059/ranjansv/frontera/exp-upsi/benchmarks/mnt/dfuse"
		PRELOAD_LIBPATH="/home1/06753/soychan/work/4NODE/BUILDS/latest/daos/install/lib64/libioil.so"
	elif grep -q "sst" <<< "$IO_NAME"; then
		ENG_TYPE="sst"
		FILENAME="output.bp"

	#Following engine types are for DAOS which don't use ADIOS2
	elif grep -q "daos-array" <<< "$IO_NAME"; then
		ENG_TYPE="daos-array"
		FILENAME="N/A"
	fi

        for DATASIZE in $DATA_PER_RANK
        do
	    #Delete previous writer*.log and reader*.log
            rm writer-*.log reader-*.log &> /dev/null
	    NR_READERS=`echo "scale=0; $NR/$READ_WRITE_RATIO" | bc`
	    echo ""
	    echo ""
            echo "Processing ${NR} writers ${NR_READERS} readers, ${ENG_TYPE}:${FILENAME}, ${DATASIZE}mb"
            #Choose PROCS and STEPS so that global array size is a whole numebr
	    GLOBAL_ARRAY_SIZE=`echo "scale=0; $DATASIZE * ($NR)" | bc`
	    echo "global array size: $GLOBAL_ARRAY_SIZE"

	    OUTPUT_DIR="$RESULT_DIR/${NR}ranks/${IO_NAME}/${DATASIZE}mb"
            mkdir -p $OUTPUT_DIR
	    #NR_READERS=$NR

	    if [[ $ENG_TYPE == "daos-array" || $ENG_TYPE == "daos-posix" ]]
            then
		    #echo "Pool UUID: $POOL_UUID"
		    #echo "List of containers"
		    #daos pool list-cont --pool=$POOL_UUID
		    echo "Destroying all containers "
		    daos pool list-cont --pool=$POOL_UUID |sed -e '1,2d'|awk '{print $1}'|xargs -L 1 -I '{}' sh -c "daos cont destroy --cont={} --pool=$POOL_UUID --force"
                    if [ $ENG_TYPE == "daos-array" ]
		    then
		        #Delete share directory contents with previous epoch values
                        mkdir -p  share/
	                rm -rf share/*
		        CONT_UUID=`daos cont create --pool=$POOL_UUID|grep -i 'created container'|awk '{print $4}'`
                        echo "New container UUID: $CONT_UUID"
		    elif [ $ENG_TYPE == "daos-posix" ]
		    then
		        CONT_UUID=`daos cont create --pool=$POOL_UUID --type=POSIX|grep -i 'created container'|awk '{print $4}'`
                        echo "New POSIX container UUID: $CONT_UUID"
		    fi
	    fi

	    export I_MPI_PIN=0

	    writer_nodes=$((($NR + $RANKS_PER_NODE - 1)/$RANKS_PER_NODE))
	    #reader_nodes=$((($NR_READERS + $RANKS_PER_NODE - 1)/$RANKS_PER_NODE))
	    i=$((writer_nodes + 1))
	    offset=$(( (i-1)*28 ))
            echo "First Reader Node: $i, offset: $offset" 


	    if [ $BENCH_TYPE == "writer" ]
	    then
	       if [ $ENG_TYPE == "daos-array" ]
	       then
                     ibrun -o 0 -n $NR numactl --cpunodebind=0 --preferred=0 build/daos_array-writer $POOL_UUID $CONT_UUID $GLOBAL_ARRAY_SIZE $STEPS &>> $OUTPUT_DIR/stdout-mpirun-writers.log
                     ibrun -o $offset -n $NR_READERS numactl --cpunodebind=0 --preferred=0 build/daos_array-reader $POOL_UUID $CONT_UUID $GLOBAL_ARRAY_SIZE $STEPS &>> $OUTPUT_DIR/stdout-mpirun-readers.log 
	       else
		   dfuse --mountpoint=$MOUNTPOINT --pool=$POOL_UUID --container=$CONT_UUID
                   ibrun -n $NR -o 0 build/writer $ENG_TYPE $FILENAME $GLOBAL_ARRAY_SIZE $STEPS &>> $OUTPUT_DIR/stdout-mpirun-writers.log
                   ibrun -n $NR_READERS -o $NR build/reader $ENG_TYPE $FILENAME $GLOBAL_ARRAY_SIZE $STEPS &>> $OUTPUT_DIR/stdout-mpirun-readers.log
                   mv writer*.log $OUTPUT_DIR/
                   mv reader*.log $OUTPUT_DIR/
		   fusermount -u $MOUNTPOINT
	       fi
	    elif [ $BENCH_TYPE == "workflow" ]
	    then
	       if [ $ENG_TYPE == "daos-array" ]
	       then
	           START_TIME=$SECONDS
                   ibrun -o 0 -n $NR numactl --cpunodebind=0 --preferred=0 build/daos_array-writer $POOL_UUID $CONT_UUID $GLOBAL_ARRAY_SIZE $STEPS &>> $OUTPUT_DIR/stdout-mpirun-writers.log &
                   ibrun -o $offset -n $NR_READERS numactl --cpunodebind=0 --preferred=0 build/daos_array-reader $POOL_UUID $CONT_UUID $GLOBAL_ARRAY_SIZE $STEPS &>> $OUTPUT_DIR/stdout-mpirun-readers.log &
		   wait
	           ELAPSED_TIME=$(($SECONDS - $START_TIME))

                   #mv writer*.log $OUTPUT_DIR/
                   #mv reader*.log $OUTPUT_DIR/
	           echo "$ELAPSED_TIME" > $OUTPUT_DIR/workflow-time.log
	       elif [ $ENG_TYPE == "daos-posix" ]
	       then
		   export TACC_TASKS_PER_NODE=1
		   ibrun -np $SLURM_JOB_NUM_NODES  dfuse --mountpoint=$MOUNTPOINT --pool=$POOL_UUID --container=$CONT_UUID
		   unset TACC_TASKS_PER_NODE
	           START_TIME=$SECONDS
                   #env LD_PRELOAD=$PRELOAD_LIBPATH ibrun -n $NR -o 0 build/writer $ENG_TYPE $FILENAME $GLOBAL_ARRAY_SIZE $STEPS &>> $OUTPUT_DIR/stdout-mpirun-writers.log &
                   #env LD_PRELOAD=$PRELOAD_LIBPATH ibrun -n $NR_READERS -o $NR build/reader $ENG_TYPE $FILENAME $GLOBAL_ARRAY_SIZE $STEPS &>> $OUTPUT_DIR/stdout-mpirun-readers.log
                   #ibrun -n $NR -o 0 build/writer $ENG_TYPE $FILENAME $GLOBAL_ARRAY_SIZE $STEPS &>> $OUTPUT_DIR/stdout-mpirun-writers.log &
                   #ibrun -n $NR_READERS -o $NR build/reader $ENG_TYPE $FILENAME $GLOBAL_ARRAY_SIZE $STEPS &>> $OUTPUT_DIR/stdout-mpirun-readers.log

                   ibrun -o 0 -n $NR  numactl --cpunodebind=0 --preferred=0 env LD_PRELOAD=$PRELOAD_LIBPATH build/writer $ENG_TYPE $FILENAME $GLOBAL_ARRAY_SIZE $STEPS &>> $OUTPUT_DIR/stdout-mpirun-writers.log &
                   ibrun -o $offset -n $NR_READERS  numactl --cpunodebind=0 --preferred=0 env LD_PRELOAD=$PRELOAD_LIBPATH build/reader $ENG_TYPE $FILENAME $GLOBAL_ARRAY_SIZE $STEPS &>> $OUTPUT_DIR/stdout-mpirun-readers.log &
		   wait
	           ELAPSED_TIME=$(($SECONDS - $START_TIME))

                   mv writer*.log $OUTPUT_DIR/
                   mv reader*.log $OUTPUT_DIR/
	           echo "$ELAPSED_TIME" > $OUTPUT_DIR/workflow-time.log
		   export TACC_TASKS_PER_NODE=1
		   ibrun -np $SLURM_JOB_NUM_NODES fusermount -u $MOUNTPOINT
		   unset TACC_TASKS_PER_NODE
	       else
	           rm ./output.bp.sst
		   export SstVerbose=2
	           START_TIME=$SECONDS
                   ibrun -o 0 -n $NR numactl --cpunodebind=0 --preferred=0 build/writer $ENG_TYPE $FILENAME $GLOBAL_ARRAY_SIZE $STEPS &>> $OUTPUT_DIR/stdout-mpirun-writers.log &
                   ibrun -o $offset -n $NR_READERS numactl --cpunodebind=0 --preferred=0  build/reader $ENG_TYPE $FILENAME $GLOBAL_ARRAY_SIZE $STEPS &>> $OUTPUT_DIR/stdout-mpirun-readers.log &
		   wait
	           ELAPSED_TIME=$(($SECONDS - $START_TIME))
		   unset SstVerbose

                   mv writer*.log $OUTPUT_DIR/
                   mv reader*.log $OUTPUT_DIR/
	           echo "$ELAPSED_TIME" > $OUTPUT_DIR/workflow-time.log
	       fi
	    fi
        done
    done
done

./daos-destoy-cont.sh
./parse-result.sh $RESULT_DIR

echo "CSV directory:"
echo "$RESULT_DIR/csv"

#cat $RESULT_DIR/csv/*.csv
mkdir -p "export-${RESULT_DIR}/csv/"
cp $RESULT_DIR/csv/*.csv export-${RESULT_DIR}/csv/
cp ${CONFIG_FILE} export-$RESULT_DIR/config.sh
cp ./adios2.xml export-$RESULT_DIR
cp ./$SCRIPT_NAME export-$RESULT_DIR
mount|grep dax > export-$RESULT_DIR/fs-type.log
ls -1t upsi*|head -2|xargs -L 1 -I {} sh -c "mv {} results/latest/"


echo "List of stdout files with error"
find $RESULT_DIR/ -iname 'stdout*.log'|xargs ls -1t|tac|xargs grep -il 'error'

#source ./daos-list-cont.sh

