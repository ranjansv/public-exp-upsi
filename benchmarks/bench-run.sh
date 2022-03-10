#!/bin/bash
#SBATCH -J upsi-bench           # Job name
#SBATCH -o upsi-bench.o%j       # Name of stdout output file
#SBATCH -e upsi-bench.e%j       # Name of stderr error file
#SBATCH -p normal 	# Queue (partition) name
#SBATCH -N 37                # Total # of nodes 
#SBATCH -n 1036		# Total # of mpi tasks
#SBATCH --ntasks-per-node=28
#SBATCH -t 16:00:00        # Run time (hh:mm:ss)
#SBATCH --mail-type=all    # Send email at begin and end of job
#SBATCH --mail-user=ranjansv@gmail.com

POOL_UUID=`dmg -i -o $daos_config pool list --verbose | tail -2|head -1|awk '{print $2}'`
echo "POOL_UUID: $POOL_UUID"
echo "SLURM_JOB_NUM_NODES: $SLURM_JOB_NUM_NODES"

CONFIG_FILE=$1

#Load config file
source ${CONFIG_FILE}

#Create timestamped output directory
TIMESTAMP=`echo $(date +%Y-%m-%d-%H:%M:%S)`
RESULT_DIR="results/$TIMESTAMP"
mkdir -p $RESULT_DIR

echo "Result dir: $RESULT_DIR"

echo "ADIOS branch"
cd /home1/08059/ranjansv/ADIOS2/
git branch
cd -

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

SCRIPT_NAME="bench-run.sh"
cp ./$SCRIPT_NAME  $RESULT_DIR/



is_daos_agent_running=`pgrep daos_agent`
echo $is_daos_agent_running
if [[ $is_daos_agent_running -eq "" ]]
then
export TACC_TASKS_PER_NODE=1
ibrun -np $SLURM_JOB_NUM_NODES  ./daos_startup.sh &
unset TACC_TASKS_PER_NODE
else
   echo "daos_agent is already running"
fi

echo "Waiting for agent to initialize..."
sleep 60

RANKS_PER_NODE=28
echo "Staring tests"


  for NR in $PROCS
  do
      writer_nodes=$((($NR + $RANKS_PER_NODE - 1)/$RANKS_PER_NODE))
      for IO_NAME in $ENGINE
      do
  	#Parse IO_NAME for engine and storage type in case of DAOS
  	if grep -q "daos-posix" <<< "$IO_NAME"; then
  		ENG_TYPE="daos-posix"
  		FILENAME="/tmp/dfuse/output.bp"
  		MOUNTPOINT="/tmp/dfuse"
  		PRELOAD_LIBPATH="/work2/08126/dbohninx/frontera/4NODE/BUILDS/latest/daos/install/lib64/libioil.so"
  	#Following engine types are for DAOS which don't use ADIOS2
  	elif grep -q "daos-array" <<< "$IO_NAME"; then
  		ENG_TYPE="daos-array"
  		FILENAME="N/A"
  	elif grep -q "lustre-posix" <<< "$IO_NAME"; then
  		ENG_TYPE="lustre-posix"
  		FILENAME="./mnt/lustre/"
  	fi
  
          for DATASIZE in $DATA_PER_RANK
          do
	    TOTAL_DATA_SIZE=`echo "scale=0; $DATASIZE * ($NR) * $STEPS" | bc`
	    if [ $TOTAL_DATA_SIZE != $PRESET_TOTAL_DATA_SIZE ]
	    then
	    	continue
	    fi
  	    echo ""
  	    echo ""
            echo "Processing ${NR} writers , ${ENG_TYPE}:${FILENAME}, ${DATASIZE}mb"
              #Choose PROCS and STEPS so that global array size is a whole numebr
  	    GLOBAL_ARRAY_SIZE=`echo "scale=0; $DATASIZE * ($NR)" | bc`
  	    echo "global array size: $GLOBAL_ARRAY_SIZE"
  
  	    export I_MPI_PIN=0
  
  
  	    if [ $BENCH_TYPE == "writer" ]
  	    then
  	       if [ $ENG_TYPE == "daos-array" ]
  	       then
  		    echo "Destroying previous containers, if any "
  		    daos pool list-cont --pool=$POOL_UUID |sed -e '1,2d'|awk '{print $1}'|xargs -L 1 -I '{}' sh -c "daos cont destroy --cont={} --pool=$POOL_UUID --force"

  		    #Delete share directory contents with previous epoch values
                    mkdir -p  share/
  	            rm -rf share/*
  		    echo "0" > share/snapshot_count.txt
  		    echo "0" > share/oid_part_count.txt
  		    CONT_UUID=`daos cont create --pool=$POOL_UUID|grep -i 'created container'|awk '{print $4}'`
                   echo "New container UUID: $CONT_UUID"
	           OUTPUT_DIR="$RESULT_DIR/${NR}ranks/${DATASIZE}mb/${IO_NAME}/"
		   mkdir -p $OUTPUT_DIR
		   export I_MPI_ROOT=/opt/intel/compilers_and_libraries_2020.4.304/linux/mpi
		   export TACC_MPI_GETMODE=impi_hydra
  	           START_TIME=$SECONDS
                     ibrun -o 0 -n $NR numactl --cpunodebind=0 --preferred=0 env CALI_CONFIG=runtime-report  build/daos_array-writer $POOL_UUID $CONT_UUID $GLOBAL_ARRAY_SIZE $STEPS &>> $OUTPUT_DIR/stdout-mpirun-writers.log
                     #ibrun -o 0 -n $NR   env CALI_CONFIG="hatchet-sample-profile(output=$OUTPUT_DIR/daos_array-writer-${NR}ranks-${DATASIZE}mb.json)"  build/daos_array-writer $POOL_UUID $CONT_UUID $GLOBAL_ARRAY_SIZE $STEPS &>> $OUTPUT_DIR/stdout-mpirun-writers.log
  	           ELAPSED_TIME=$(($SECONDS - $START_TIME))
		   unset I_MPI_ROOT
		   unset TACC_MPI_GETMODE
  
  	           echo "$ELAPSED_TIME" > $OUTPUT_DIR/workflow-time.log
  	       elif [ $ENG_TYPE == "daos-posix" ]
  	       then
  		   echo "Destroying previous containers, if any "
  		   daos pool list-cont --pool=$POOL_UUID |sed -e '1,2d'|awk '{print $1}'|xargs -L 1 -I '{}' sh -c "daos cont destroy --cont={} --pool=$POOL_UUID --force"
  		   CONT_UUID=`daos cont create --pool=$POOL_UUID --type=POSIX|grep -i 'created container'|awk '{print $4}'`
                   echo "New POSIX container UUID: $CONT_UUID"

		   echo "Cleaning up stale daos-posix mounts, if any"
  		   export TACC_TASKS_PER_NODE=1
  		   ibrun -np $SLURM_JOB_NUM_NODES fusermount3 -u $MOUNTPOINT
  		   unset TACC_TASKS_PER_NODE
		   echo "Mounting daos-posix using dfuse"

  		   export TACC_TASKS_PER_NODE=1
  		   #ibrun -np $SLURM_JOB_NUM_NODES  dfuse --mountpoint=$MOUNTPOINT --pool=$POOL_UUID --container=$CONT_UUID &
  		   ibrun -np $SLURM_JOB_NUM_NODES launch_dfuse.sh $MOUNTPOINT $POOL_UUID $CONT_UUID &
  		   unset TACC_TASKS_PER_NODE

  		   #ibrun waits for dfuse deamon which not return and hangs up without the &. Hence, we run ibrun in background and put a sleep so that dfuse moun
  		   #is complete on all nodes
		   sleep 120

                  for ADIOS_XML in $AGGREGATORS
                  do
		      echo ""
		      echo "Aggregator: $ADIOS_XML"
                      cp adios-config/${ADIOS_XML}.xml adios2.xml
	              OUTPUT_DIR="$RESULT_DIR/${NR}ranks/${DATASIZE}mb/${IO_NAME}/${ADIOS_XML}/"

                      #Save ADIOS config for each aggregator
                      mkdir -p $OUTPUT_DIR
                      cp adios-config/${ADIOS_XML}.xml $OUTPUT_DIR/

		      echo "Running daos-posix workload"
		      export I_MPI_ROOT=/opt/intel/compilers_and_libraries_2020.4.304/linux/mpi
		      export TACC_MPI_GETMODE=impi_hydra
  
  	              START_TIME=$SECONDS
                      #ibrun -n $NR -o 0 build/writer $ENG_TYPE $FILENAME $GLOBAL_ARRAY_SIZE $STEPS &>> $OUTPUT_DIR/stdout-mpirun-writers.log
  
                      ibrun -o 0 -n $NR  numactl --cpunodebind=0 --preferred=0  env CALI_CONFIG=runtime-report LD_PRELOAD=$PRELOAD_LIBPATH build/writer posix $OUTPUT_DIR $FILENAME $GLOBAL_ARRAY_SIZE $STEPS &>> $OUTPUT_DIR/stdout-mpirun-writers.log
  	              ELAPSED_TIME=$(($SECONDS - $START_TIME))

  		      rm -rf $FILENAME/* &> /dev/null 
		      unset I_MPI_ROOT
		      unset TACC_MPI_GETMODE

  	              echo "$ELAPSED_TIME" > $OUTPUT_DIR/workflow-time.log
		   done
		   echo "Unmounting daos-posix"
  		   export TACC_TASKS_PER_NODE=1
  		   ibrun -np $SLURM_JOB_NUM_NODES fusermount3 -u $MOUNTPOINT
  		   unset TACC_TASKS_PER_NODE
  		   echo "Cleaning up daos-posix container"
  		   daos pool list-cont --pool=$POOL_UUID |sed -e '1,2d'|awk '{print $1}'|xargs -L 1 -I '{}' sh -c "daos cont destroy --cont={} --pool=$POOL_UUID --force"
  	       elif [ $ENG_TYPE == "lustre-posix" ]
  	       then
                  for ADIOS_XML in $AGGREGATORS
                  do
		   echo ""
		   echo "Aggregator: $ADIOS_XML"
                   cp adios-config/${ADIOS_XML}.xml adios2.xml
	           OUTPUT_DIR="$RESULT_DIR/${NR}ranks/${DATASIZE}mb/${IO_NAME}/${ADIOS_XML}/"

                   #Save ADIOS config for each aggregator
                   mkdir -p $OUTPUT_DIR
  		   rm -rf ./mnt/lustre/* &> /dev/null

		   export I_MPI_ROOT=/opt/intel/compilers_and_libraries_2020.4.304/linux/mpi
		   export TACC_MPI_GETMODE=impi_hydra
  	           START_TIME=$SECONDS
                     ibrun -o 0 -n $NR  numactl --cpunodebind=0 --preferred=0 env CALI_CONFIG=runtime-report build/writer posix $OUTPUT_DIR $FILENAME $GLOBAL_ARRAY_SIZE $STEPS &>> $OUTPUT_DIR/stdout-mpirun-writers.log
  	           ELAPSED_TIME=$(($SECONDS - $START_TIME))

		   unset I_MPI_ROOT
		   unset TACC_MPI_GETMODE
  	           echo "$ELAPSED_TIME" > $OUTPUT_DIR/workflow-time.log
  		   #rm -rf ./mnt/lustre/* &> /dev/null
  		   echo "Listing Lustre files"
  		   ls -lh ./mnt/lustre/
  		   rm -rf ./mnt/lustre/* &> /dev/null
		   done
  	       fi
  	    fi
          done
      done
done

./daos-destroy-cont.sh
echo "Generating CSV files"
./parse-result.sh $RESULT_DIR

echo "CSV directory:"
echo "$RESULT_DIR/csv"

#cat $RESULT_DIR/csv/*.csv
mkdir -p "export-${RESULT_DIR}/csv/"
cp $RESULT_DIR/csv/*.csv export-${RESULT_DIR}/csv/
cp ${CONFIG_FILE} export-$RESULT_DIR/config.sh
cp ./$SCRIPT_NAME export-$RESULT_DIR
ls -1t upsi*|head -2|xargs -L 1 -I {} sh -c "mv {} results/latest/"


echo "List of stdout files with error"
find $RESULT_DIR/ -iname 'stdout*.log'|xargs ls -1t|tac|xargs grep -il 'error'

#source ./daos-list-cont.sh

