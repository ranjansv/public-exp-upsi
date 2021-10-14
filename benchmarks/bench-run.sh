#!/bin/bash
#SBATCH -J upsi-bench           # Job name
#SBATCH -o upsi-bench.o%j       # Name of stdout output file
#SBATCH -e upsi-bench.e%j       # Name of stderr error file
#SBATCH -p small                # Queue (partition) name
#SBATCH -N 2               # Total # of nodes 
#SBATCH -n 48              # Total # of mpi tasks
#SBATCH -t 00:05:00        # Run time (hh:mm:ss)
#SBATCH --mail-type=all    # Send email at begin and end of job
#SBATCH --mail-user=ranjansv@gmail.com

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
git branch --show-current > git branch --show-current
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
   $HOME/bin/daos_startup.sh
else
   echo "daos_agent is already running"
fi


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

	    if [ $BENCH_TYPE == "writer" ]
	    then
	       if [ $ENG_TYPE == "daos-array" ]
	       then
                     ibrun -n $NR -o 0 build/daos_array-writer $POOL_UUID $CONT_UUID $GLOBAL_ARRAY_SIZE $STEPS &>> $OUTPUT_DIR/stdout-mpirun-writers.log 
                     ibrun -n $NR_READERS -o $NR build/daos_array-reader $POOL_UUID $CONT_UUID $GLOBAL_ARRAY_SIZE $STEPS &>> $OUTPUT_DIR/stdout-mpirun-readers.log
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
                   ibrun -n $NR -o 0 build/daos_array-writer $POOL_UUID $CONT_UUID $GLOBAL_ARRAY_SIZE $STEPS &>> $OUTPUT_DIR/stdout-mpirun-writers.log &
                   ibrun -n $NR_READERS -o $NR build/daos_array-reader $POOL_UUID $CONT_UUID $GLOBAL_ARRAY_SIZE $STEPS &>> $OUTPUT_DIR/stdout-mpirun-readers.log
	           ELAPSED_TIME=$(($SECONDS - $START_TIME))

                   #mv writer*.log $OUTPUT_DIR/
                   #mv reader*.log $OUTPUT_DIR/
	           echo "$ELAPSED_TIME" > $OUTPUT_DIR/workflow-time.log
	       elif [ $ENG_TYPE == "daos-posix" ]
	       then
		   dfuse --mountpoint=$MOUNTPOINT --pool=$POOL_UUID --container=$CONT_UUID
		   PID=`pgrep dfuse`
		   echo "dfuse pid: $PID"
	           START_TIME=$SECONDS
                   #env LD_PRELOAD=$PRELOAD_LIBPATH ibrun -n $NR -o 0 build/writer $ENG_TYPE $FILENAME $GLOBAL_ARRAY_SIZE $STEPS &>> $OUTPUT_DIR/stdout-mpirun-writers.log &
                   #env LD_PRELOAD=$PRELOAD_LIBPATH ibrun -n $NR_READERS -o $NR build/reader $ENG_TYPE $FILENAME $GLOBAL_ARRAY_SIZE $STEPS &>> $OUTPUT_DIR/stdout-mpirun-readers.log
                   #ibrun -n $NR -o 0 build/writer $ENG_TYPE $FILENAME $GLOBAL_ARRAY_SIZE $STEPS &>> $OUTPUT_DIR/stdout-mpirun-writers.log &
                   #ibrun -n $NR_READERS -o $NR build/reader $ENG_TYPE $FILENAME $GLOBAL_ARRAY_SIZE $STEPS &>> $OUTPUT_DIR/stdout-mpirun-readers.log

                   ibrun -n $NR -o 0 env LD_PRELOAD=$PRELOAD_LIBPATH build/writer $ENG_TYPE $FILENAME $GLOBAL_ARRAY_SIZE $STEPS &>> $OUTPUT_DIR/stdout-mpirun-writers.log &
                   ibrun -n $NR_READERS -o $NR env LD_PRELOAD=$PRELOAD_LIBPATH build/reader $ENG_TYPE $FILENAME $GLOBAL_ARRAY_SIZE $STEPS &>> $OUTPUT_DIR/stdout-mpirun-readers.log
	           ELAPSED_TIME=$(($SECONDS - $START_TIME))

                   mv writer*.log $OUTPUT_DIR/
                   mv reader*.log $OUTPUT_DIR/
	           echo "$ELAPSED_TIME" > $OUTPUT_DIR/workflow-time.log
		   fusermount -u $MOUNTPOINT
	       else
	           rm ./output.bp.sst
	           START_TIME=$SECONDS

                   ibrun -n $NR -o 0 build/writer $ENG_TYPE $FILENAME $GLOBAL_ARRAY_SIZE $STEPS &>> $OUTPUT_DIR/stdout-mpirun-writers.log &
                   ibrun -n $NR_READERS -o $NR build/reader $ENG_TYPE $FILENAME $GLOBAL_ARRAY_SIZE $STEPS &>> $OUTPUT_DIR/stdout-mpirun-readers.log
	           ELAPSED_TIME=$(($SECONDS - $START_TIME))

                   mv writer*.log $OUTPUT_DIR/
                   mv reader*.log $OUTPUT_DIR/
	           echo "$ELAPSED_TIME" > $OUTPUT_DIR/workflow-time.log
	       fi
	    fi
        done
    done
done

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


echo "List of stdout files with error"
find $RESULT_DIR/ -iname 'stdout*.log'|xargs ls -1t|tac|xargs grep -il 'error'

#source ./daos-list-cont.sh

