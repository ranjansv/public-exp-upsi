
POOL_UUID=`dmg -i -o $daos_config pool list --verbose | tail -2|head -1|awk '{print $2}'`
echo "POOL_UUID: $POOL_UUID"
echo "SLURM_JOB_NUM_NODES: $SLURM_JOB_NUM_NODES"
SLURM_JOB_CPUS_PER_NODE=28
echo "SLURM_JOB_CPUS_PER_NODE: $SLURM_JOB_CPUS_PER_NODE"

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

SCRIPT_NAME="singlenode-bench-run.sh"
cp ./$SCRIPT_NAME  $RESULT_DIR/


export I_MPI_PIN=0
export I_MPI_ROOT=/opt/intel/compilers_and_libraries_2020.4.304/linux/mpi
export TACC_MPI_GETMODE=impi_hydra

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
sleep 6

echo "Staring tests"


for NR in $PROCS
do
      writer_nodes=$((($NR + $SLURM_JOB_CPUS_PER_NODE - 1)/$SLURM_JOB_CPUS_PER_NODE))
      NR_READERS=`echo "scale=0; $NR/$READ_WRITE_RATIO" | bc`
      offset=$(( $writer_nodes * $SLURM_JOB_CPUS_PER_NODE ))
      echo "First reader node: $writer_nodes, reader offset: $offset" 

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
  	    echo ""
  	    echo ""
            echo "Processing ${NR} writers , ${ENG_TYPE}:${FILENAME}, ${DATASIZE}mb"
              #Choose PROCS and STEPS so that global array size is a whole numebr
  	    GLOBAL_ARRAY_SIZE=`echo "scale=0; $DATASIZE * ($NR)" | bc`
  	    echo "global array size: $GLOBAL_ARRAY_SIZE"
  

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
  	           if [ $BENCH_TYPE == "writer-reader" ]
  	           then
  	               START_TIME=$SECONDS
                         ibrun -o 0 -n $NR numactl --cpunodebind=0 --preferred=0 env CALI_CONFIG=runtime-report,calc.inclusive  build/daos_array-writer $POOL_UUID $CONT_UUID $GLOBAL_ARRAY_SIZE $STEPS &>> $OUTPUT_DIR/stdout-mpirun-writers.log
  	               ELAPSED_TIME=$(($SECONDS - $START_TIME))
  	               echo "$ELAPSED_TIME" > $OUTPUT_DIR/writeworkflow-time.log

		       #read -n 1 -r -s -p $'Press enter to continue...\n'

  	               START_TIME=$SECONDS
                         ibrun -o 0 -n $NR_READERS numactl --cpunodebind=0 --preferred=0 env CALI_CONFIG=runtime-report,calc.inclusive  build/daos_array-reader $POOL_UUID $CONT_UUID $GLOBAL_ARRAY_SIZE $READ_IO_SIZE $STEPS &>> $OUTPUT_DIR/stdout-mpirun-readers.log
  	               ELAPSED_TIME=$(($SECONDS - $START_TIME))
  	               echo "$ELAPSED_TIME" > $OUTPUT_DIR/readworkflow-time.log
                   elif [ $BENCH_TYPE == "workflow" ]
		   then
  	               START_TIME=$SECONDS
                         ibrun -o 0 -n $NR numactl --cpunodebind=0 --preferred=0 env CALI_CONFIG=runtime-report,calc.inclusive  build/daos_array-writer $POOL_UUID $CONT_UUID $GLOBAL_ARRAY_SIZE $STEPS &>> $OUTPUT_DIR/stdout-mpirun-writers.log &        
                         ibrun -o 0 -n $NR_READERS numactl --cpunodebind=0 --preferred=0 env CALI_CONFIG=runtime-report,calc.inclusive  build/daos_array-reader $POOL_UUID $CONT_UUID $GLOBAL_ARRAY_SIZE $READ_IO_SIZE $STEPS &>> $OUTPUT_DIR/stdout-mpirun-readers.log
  	               ELAPSED_TIME=$(($SECONDS - $START_TIME))
  	               echo "$ELAPSED_TIME" > $OUTPUT_DIR/workflow-time.log
		   fi

  	       elif [ $ENG_TYPE == "daos-posix" ]
  	       then
  		   echo "Destroying previous containers, if any "
  		   daos pool list-cont --pool=$POOL_UUID |sed -e '1,2d'|awk '{print $1}'|xargs -L 1 -I '{}' sh -c "daos cont destroy --cont={} --pool=$POOL_UUID --force"
  		   CONT_UUID=`daos cont create --pool=$POOL_UUID --type=POSIX|grep -i 'created container'|awk '{print $4}'`
                   echo "New POSIX container UUID: $CONT_UUID"

		   echo ""
		   echo "Cleaning up stale daos-posix mounts, if any"
  		   export TACC_TASKS_PER_NODE=1
  		   ibrun -np $SLURM_JOB_NUM_NODES fusermount3 -u $MOUNTPOINT
  		   unset TACC_TASKS_PER_NODE

		   echo ""
		   echo "Mounting daos-posix using dfuse"
  		   export TACC_TASKS_PER_NODE=1
  		   ibrun -np $SLURM_JOB_NUM_NODES launch_dfuse.sh $MOUNTPOINT $POOL_UUID $CONT_UUID &
  		   unset TACC_TASKS_PER_NODE

  		   #ibrun waits for dfuse deamon which not return and hangs up without the &. Hence, we run ibrun in background and put a sleep so that dfuse moun
  		   #is complete on all nodes
		   sleep 6

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

		      if [ $BENCH_TYPE == "writer-reader" ]
		      then
		          echo "Starting writers"
  	                  START_TIME=$SECONDS
                          ibrun -o 0 -n $NR  numactl --cpunodebind=0 --preferred=0  env CALI_CONFIG=runtime-report,calc.inclusive LD_PRELOAD=$PRELOAD_LIBPATH build/writer posix $FILENAME $GLOBAL_ARRAY_SIZE $STEPS &>> $OUTPUT_DIR/stdout-mpirun-writers.log
  	                  ELAPSED_TIME=$(($SECONDS - $START_TIME))
  	                  echo "$ELAPSED_TIME" > $OUTPUT_DIR/writeworkflow-time.log

		          #read -n 1 -r -s -p $'Press enter to continue...\n'

		          echo "Starting readers with read io size(bytes): $READ_IO_SIZE"
  	                  START_TIME=$SECONDS
                          #ibrun -o 0 -n $NR_READERS  numactl --cpunodebind=0 --preferred=0  strace build/reader posix $FILENAME $READ_IO_SIZE &>> $OUTPUT_DIR/stdout-mpirun-readers.log
                          ibrun -o 0 -n $NR_READERS  numactl --cpunodebind=0 --preferred=0  env CALI_CONFIG=runtime-report,calc.inclusive LD_PRELOAD=$PRELOAD_LIBPATH build/reader posix $FILENAME $READ_IO_SIZE &>> $OUTPUT_DIR/stdout-mpirun-readers.log
  	                  ELAPSED_TIME=$(($SECONDS - $START_TIME))
  	                  echo "$ELAPSED_TIME" > $OUTPUT_DIR/readworkflow-time.log

		      elif [ $BENCH_TYPE == "workflow" ]
		      then
  	                  START_TIME=$SECONDS
                          ibrun -o 0 -n $NR  numactl --cpunodebind=0 --preferred=0  env CALI_CONFIG=runtime-report,calc.inclusive LD_PRELOAD=$PRELOAD_LIBPATH build/writer posix $FILENAME $GLOBAL_ARRAY_SIZE $STEPS &>> $OUTPUT_DIR/stdout-mpirun-writers.log &    
                          ibrun -o 0 -n $NR_READERS  numactl --cpunodebind=0 --preferred=0  env CALI_CONFIG=runtime-report,calc.inclusive LD_PRELOAD=$PRELOAD_LIBPATH build/reader posix $FILENAME $READ_IO_SIZE &>> $OUTPUT_DIR/stdout-mpirun-readers.log
  	                  ELAPSED_TIME=$(($SECONDS - $START_TIME))
  	                  echo "$ELAPSED_TIME" > $OUTPUT_DIR/workflow-time.log
		      fi

  		      rm -rf $FILENAME/* &> /dev/null 

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

  	           START_TIME=$SECONDS
                     ibrun -o 0 -n $NR  numactl --cpunodebind=0 --preferred=0 env CALI_CONFIG=runtime-report,calc.inclusive build/writer posix $FILENAME $GLOBAL_ARRAY_SIZE $STEPS &>> $OUTPUT_DIR/stdout-mpirun-writers.log
  	           ELAPSED_TIME=$(($SECONDS - $START_TIME))

  	           echo "$ELAPSED_TIME" > $OUTPUT_DIR/workflow-time.log

  		   echo "Listing Lustre files"
  		   ls -lh ./mnt/lustre/
  		   rm -rf ./mnt/lustre/* &> /dev/null
		   done
  	       fi
  	    
          done
      done
done

echo "Destroying previous containers, if any "
daos pool list-cont --pool=$POOL_UUID |sed -e '1,2d'|awk '{print $1}'|xargs -L 1 -I '{}' sh -c "daos cont destroy --cont={} --pool=$POOL_UUID --force"
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

