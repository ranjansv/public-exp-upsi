POOL_UUID=$(dmg -i -o $daos_config pool list --verbose | grep ranjan | awk '{print $2}')
echo "POOL_UUID: $POOL_UUID"
echo "SLURM_JOB_NUM_NODES: $SLURM_JOB_NUM_NODES"
SLURM_JOB_CPUS_PER_NODE=28
echo "SLURM_JOB_CPUS_PER_NODE: $SLURM_JOB_CPUS_PER_NODE"

CONFIG_FILE=$1

#Load config file
source ${CONFIG_FILE}

#Create timestamped output directory
TIMESTAMP=$(echo $(date +%Y-%m-%d-%H:%M:%S))
RESULT_DIR="results/$TIMESTAMP"
mkdir -p $RESULT_DIR

echo "Result dir: $RESULT_DIR"

echo "ADIOS branch"
cd /home1/08059/ranjansv/ADIOS2/
git branch
git log | head
cd -


rm results/latest
ln -s $TIMESTAMP results/latest

#mount > $RESULT_DIR/fs-mounts.log
git branch --show-current >$RESULT_DIR/git.log
git log --format="%H" -n 1 >>$RESULT_DIR/git.log

daos version
echo "daos_config: $daos_config"
daos version > $RESULT_DIR/daos_version.log

#Build source
cd build
make clean && make
cd ..

#Copy configs and xml to outputdir
cp ${CONFIG_FILE} $RESULT_DIR/config.sh

SCRIPT_NAME="bench-run.sh"
cp ./$SCRIPT_NAME $RESULT_DIR/

export I_MPI_PIN=0
export I_MPI_ROOT=/opt/intel/compilers_and_libraries_2020.4.304/linux/mpi
export TACC_MPI_GETMODE=impi_hydra

is_daos_agent_running=$(pgrep daos_agent)
echo $is_daos_agent_running
if [[ $is_daos_agent_running -eq "" ]]; then
	export TACC_TASKS_PER_NODE=1
	ibrun -np $SLURM_JOB_NUM_NODES ./daos_startup.sh &
	unset TACC_TASKS_PER_NODE
else
	echo "daos_agent is already running"
fi

echo "Waiting for agent to initialize..."
sleep 6

echo "benchmark type: $BENCH_TYPE"

for NR in $PROCS; do
    	offset=$NR
    
    	NR_READERS=$(echo "scale=0; $NR/$READ_WRITE_RATIO" | bc)
    
    	for IO_NAME in $ENGINE; do
    		#Parse IO_NAME for engine and storage type in case of DAOS
    		if grep -q "adios+daos-posix" <<<"$IO_NAME"; then
    			ENG_TYPE="adios+daos-posix"
    			FILENAME="/tmp/dfuse/output.bp"
    			MOUNTPOINT="/tmp/dfuse"
    			PRELOAD_LIBPATH="/work2/08126/dbohninx/frontera/4NODE/BUILDS/latest/daos/install/lib64/libioil.so"
    		#Following engine types are for DAOS which don't use ADIOS2
    		elif grep -q "ior+dfs" <<<"$IO_NAME"; then
    			ENG_TYPE="ior+dfs"
    			FILENAME="/testfile"
    		elif grep -q "ior+daos-posix" <<<"$IO_NAME"; then
    			ENG_TYPE="ior+daos-posix"
    			FILENAME="/tmp/dfuse/testfile"
    			MOUNTPOINT="/tmp/dfuse"
    			PRELOAD_LIBPATH="/work2/08126/dbohninx/frontera/4NODE/BUILDS/latest/daos/install/lib64/libioil.so"
    		elif grep -q "daos_array" <<<"$IO_NAME"; then
    			ENG_TYPE=$IO_NAME
    			FILENAME="N/A"
    		fi
    
    		for DATASIZE in $DATA_PER_RANK; do
		    for NUM_ADIOS_VAR in $LIST_NUM_ADIOS_VAR; do
    			echo ""
    			echo ""
    			echo "Processing $NUM_ADIOS_VAR vars, ${ENG_TYPE}:${FILENAME}, ${DATASIZE}mb with $NR writers and $NR_READERS readers"
    			#Choose PROCS and STEPS so that global array size is a whole numebr
    			GLOBAL_ARRAY_SIZE=$(echo "scale=0; $DATASIZE * ($NR)" | bc)
    			echo "global array size: $GLOBAL_ARRAY_SIZE"

			PUT_SIZE=$(echo "scale=0; $DATASIZE / $NUM_ADIOS_VAR" | bc)
			PUT_SIZE_BYTES=$(echo "scale=0; $PUT_SIZE * 1024 * 1024" | bc)
			echo "Per ADIOS variable PUT_SIZE: ${PUT_SIZE}MB"
    
    			if [ $ENG_TYPE == "daos_array_per_adios_var" ]; then
    				echo "Destroying previous containers, if any "
    				daos pool list-cont --pool=$POOL_UUID | sed -e '1,2d' | awk '{print $1}' | xargs -L 1 -I '{}' sh -c "daos cont destroy --cont={} --pool=$POOL_UUID --force"
    
    				#Delete share directory contents with previous epoch values
    				mkdir -p share/
    				rm -rf share/*
    				echo "0" >share/snapshot_count.txt
    				echo "0" >share/oid_part_count.txt
    				CONT_UUID=$(daos cont create --pool=$POOL_UUID | grep -i 'created container' | awk '{print $4}')
    				echo "New container UUID: $CONT_UUID"
    				OUTPUT_DIR="$RESULT_DIR/${NR}ranks/${DATASIZE}mb/${IO_NAME}/${NUM_ADIOS_VAR}vars"
    				mkdir -p $OUTPUT_DIR
    				if [ $BENCH_TYPE == "writer-reader" ]; then
    					echo "Starting writers"
    					ibrun -o 0 -n $NR numactl --cpunodebind=0 --preferred=0 env CALI_CONFIG=runtime-report,calc.inclusive build/daos_array_per_adios_var_writer $POOL_UUID $CONT_UUID $DATASIZE $PUT_SIZE_BYTES $STEPS $NUM_ADIOS_VAR &>>$OUTPUT_DIR/stdout-mpirun-writers.log
    
    					echo "Starting readers"
    					ibrun -o 0 -n $NR_READERS numactl --cpunodebind=0 --preferred=0 env CALI_CONFIG=runtime-report,calc.inclusive build/daos_array_per_adios_var_reader $POOL_UUID $CONT_UUID $DATASIZE $STEPS $READ_WRITE_RATIO $NUM_ADIOS_VAR &>>$OUTPUT_DIR/stdout-mpirun-readers.log
					dmg -o $daos_config pool list --verbose &> $OUTPUT_DIR/pool_list.txt
    				fi
    			elif [ $ENG_TYPE == "daos_array_per_rank" ]; then
    				echo "Destroying previous containers, if any "
    				daos pool list-cont --pool=$POOL_UUID | sed -e '1,2d' | awk '{print $1}' | xargs -L 1 -I '{}' sh -c "daos cont destroy --cont={} --pool=$POOL_UUID --force"
    
    				#Delete share directory contents with previous epoch values
    				mkdir -p share/
    				rm -rf share/*
    				echo "0" >share/snapshot_count.txt
    				echo "0" >share/oid_part_count.txt
    				CONT_UUID=$(daos cont create --pool=$POOL_UUID | grep -i 'created container' | awk '{print $4}')
    				echo "New container UUID: $CONT_UUID"
    				OUTPUT_DIR="$RESULT_DIR/${NR}ranks/${DATASIZE}mb/${IO_NAME}/${NUM_ADIOS_VAR}vars"
    				mkdir -p $OUTPUT_DIR
    				if [ $BENCH_TYPE == "writer-reader" ]; then
    					echo "Starting writers"
    					ibrun -o 0 -n $NR numactl --cpunodebind=0 --preferred=0 env CALI_CONFIG=runtime-report,calc.inclusive build/daos_array_per_rank_writer $POOL_UUID $CONT_UUID $DATASIZE $STEPS $PUT_SIZE_BYTES $NUM_ADIOS_VAR &>>$OUTPUT_DIR/stdout-mpirun-writers.log
    
    					echo "Starting readers"
    				        ibrun -o 0 -n $NR_READERS numactl --cpunodebind=0 --preferred=0 env CALI_CONFIG=runtime-report,calc.inclusive build/daos_array_per_rank_reader $POOL_UUID $CONT_UUID $DATASIZE $STEPS $READ_WRITE_RATIO $PUT_SIZE_BYTES $NUM_ADIOS_VAR &>>$OUTPUT_DIR/stdout-mpirun-readers.log
					dmg -o $daos_config pool list --verbose &> $OUTPUT_DIR/pool_list.txt
    				fi
    
    			elif [ $ENG_TYPE == "adios+daos-posix" ]; then
    				echo "Destroying previous containers, if any "
    				daos pool list-cont --pool=$POOL_UUID | sed -e '1,2d' | awk '{print $1}' | xargs -L 1 -I '{}' sh -c "daos cont destroy --cont={} --pool=$POOL_UUID --force"
    				CONT_UUID=$(daos cont create --pool=$POOL_UUID --type=POSIX | grep -i 'created container' | awk '{print $4}')
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
    
    				for ADIOS_XML in $AGGREGATORS; do
    					echo ""
    					echo "Aggregator: $ADIOS_XML"
    					cp adios-config/${ADIOS_XML}.xml adios2.xml
    					OUTPUT_DIR="$RESULT_DIR/${NR}ranks/${DATASIZE}mb/${IO_NAME}/${ADIOS_XML}/${NUM_ADIOS_VAR}vars"
    
    					#Save ADIOS config for each aggregator
    					mkdir -p $OUTPUT_DIR
    					cp adios-config/${ADIOS_XML}.xml $OUTPUT_DIR/
    
    					echo "Running daos-posix workload"
    
    					if [ $BENCH_TYPE == "writer-reader" ]; then
    						echo "Starting writers"
    						ibrun -o 0 -n $NR numactl --cpunodebind=0 --preferred=0 env CALI_CONFIG=runtime-report,calc.inclusive LD_PRELOAD=$PRELOAD_LIBPATH build/adios-writer posix $FILENAME $DATASIZE $STEPS $PUT_SIZE_BYTES $NUM_ADIOS_VAR  &>>$OUTPUT_DIR/stdout-mpirun-writers.log
    
    						echo "Starting readers"
    					        ibrun -o 0 -n $NR_READERS numactl --cpunodebind=0 --preferred=0 env CALI_CONFIG=runtime-report,calc.inclusive LD_PRELOAD=$PRELOAD_LIBPATH build/adios-reader posix $FILENAME $NUM_ADIOS_VAR &>>$OUTPUT_DIR/stdout-mpirun-readers.log
					dmg -o $daos_config pool list --verbose &> $OUTPUT_DIR/pool_list.txt
    					fi
    
    					rm -rf $FILENAME/* &>/dev/null
    
    				done
    
    				echo "Unmounting daos-posix"
    				export TACC_TASKS_PER_NODE=1
    				ibrun -np $SLURM_JOB_NUM_NODES fusermount3 -u $MOUNTPOINT
    				unset TACC_TASKS_PER_NODE
    				echo "Cleaning up daos-posix container"
    				daos pool list-cont --pool=$POOL_UUID | sed -e '1,2d' | awk '{print $1}' | xargs -L 1 -I '{}' sh -c "daos cont destroy --cont={} --pool=$POOL_UUID --force"
    			elif [ $ENG_TYPE == "ior+daos-posix" ]; then
    				echo "Destroying previous containers, if any "
    				daos pool list-cont --pool=$POOL_UUID | sed -e '1,2d' | awk '{print $1}' | xargs -L 1 -I '{}' sh -c "daos cont destroy --cont={} --pool=$POOL_UUID --force"
    				CONT_UUID=$(daos cont create --pool=$POOL_UUID --type=POSIX | grep -i 'created container' | awk '{print $4}')
    				echo "New POSIX container UUID: $CONT_UUID"
    				OUTPUT_DIR="$RESULT_DIR/${NR}ranks/${DATASIZE}mb/${IO_NAME}/${NUM_ADIOS_VAR}vars"
    				mkdir -p $OUTPUT_DIR
    
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
    				if [ $BENCH_TYPE == "writer-reader" ]; then
    					echo "Starting IOR writers"
    					ibrun -o 0 -n $NR numactl --cpunodebind=0 --preferred=0 env LD_PRELOAD=$PRELOAD_LIBPATH ior -a POSIX -b ${DATASIZE}mb -t ${DATASIZE}mb -v -i $STEPS -W -w -k -o $FILENAME &>>$OUTPUT_DIR/stdout-mpirun-writers.log
    
    					TOTAL_READ_DATA=$(echo "scale=0; $DATASIZE * $READ_WRITE_RATIO" | bc)
    						#if [ $READ_PATTERN == "sequential" ]
    						#then
    						   ibrun -o 0 -n $NR_READERS numactl --cpunodebind=0 --preferred=0 env LD_PRELOAD=$PRELOAD_LIBPATH ior -a POSIX -b ${TOTAL_READ_DATA}mb -t ${TOTAL_READ_DATA}mb -v -r -i $STEPS -k -o $FILENAME &>>$OUTPUT_DIR/stdout-mpirun-readers.log
    						#else
    						   #ibrun -o 0 -n $NR_READERS numactl --cpunodebind=0 --preferred=0 env LD_PRELOAD=$PRELOAD_LIBPATH ior -a POSIX -b ${TOTAL_READ_DATA}mb -t $GET_SIZE -v -z -r -i $STEPS -k -o $FILENAME &>>$OUTPUT_DIR/stdout-mpirun-readers-iosize-$GET_SIZE.log
    						#fi
    				else
    					echo "ior+daos-posix is not currently setup for concurrent readers and writers"
    					continue
    				fi
    				echo "Unmounting daos-posix"
    				export TACC_TASKS_PER_NODE=1
    				ibrun -np $SLURM_JOB_NUM_NODES fusermount3 -u $MOUNTPOINT
    				unset TACC_TASKS_PER_NODE
    				echo "Cleaning up daos-posix container"
    				daos pool list-cont --pool=$POOL_UUID | sed -e '1,2d' | awk '{print $1}' | xargs -L 1 -I '{}' sh -c "daos cont destroy --cont={} --pool=$POOL_UUID --force"
    
                            #Currently ignored, this configuration will not be used
    			elif [ $ENG_TYPE == "ior+dfs" ]; then
    				echo "Destroying previous containers, if any "
    				daos pool list-cont --pool=$POOL_UUID | sed -e '1,2d' | awk '{print $1}' | xargs -L 1 -I '{}' sh -c "daos cont destroy --cont={} --pool=$POOL_UUID --force"
    				CONT_UUID=$(daos cont create --pool=$POOL_UUID --type=POSIX | grep -i 'created container' | awk '{print $4}')
    				echo "New POSIX container UUID: $CONT_UUID"
    				OUTPUT_DIR="$RESULT_DIR/${NR}ranks/${DATASIZE}mb/${IO_NAME}/${NUM_ADIOS_VAR}vars"
    				mkdir -p $OUTPUT_DIR
    				if [ $BENCH_TYPE == "writer-reader" ]; then
    					echo "Starting IOR writers"
    					START_TIME=$SECONDS
    					ibrun -o 0 -n $NR numactl --cpunodebind=0 --preferred=0 ior -a DFS -b ${DATASIZE}mb -t ${DATASIZE}mb -v -i $STEPS -W -w -k -o $FILENAME --dfs.pool $POOL_UUID --dfs.cont $CONT_UUID &>>$OUTPUT_DIR/stdout-mpirun-writers.log
    					ELAPSED_TIME=$(($SECONDS - $START_TIME))
    					echo "$ELAPSED_TIME" >$OUTPUT_DIR/writeworkflow-time.log
    
    					TOTAL_READ_DATA=$(echo "scale=0; $DATASIZE * $READ_WRITE_RATIO" | bc)
    					for GET_SIZE in $READ_IO_SIZE; do
    						echo "Starting IOR readers with read io size(bytes): $GET_SIZE"
    						if [ $READ_PATTERN == "sequential" ]
    						then
    						    START_TIME=$SECONDS
    						    ibrun -o 0 -n $NR_READERS numactl --cpunodebind=0 --preferred=0 ior -a DFS -b ${TOTAL_READ_DATA}mb -t $GET_SIZE -v -r -i $STEPS -k -o $FILENAME --dfs.pool $POOL_UUID --dfs.cont $CONT_UUID &>>$OUTPUT_DIR/stdout-mpirun-readers-iosize-$GET_SIZE.log
    						    ELAPSED_TIME=$(($SECONDS - $START_TIME))
    						else
    						    START_TIME=$SECONDS
    						    ibrun -o 0 -n $NR_READERS numactl --cpunodebind=0 --preferred=0 ior -a DFS -b ${TOTAL_READ_DATA}mb -t $GET_SIZE -v -z -r -i $STEPS -k -o $FILENAME --dfs.pool $POOL_UUID --dfs.cont $CONT_UUID &>>$OUTPUT_DIR/stdout-mpirun-readers-iosize-$GET_SIZE.log
    						    ELAPSED_TIME=$(($SECONDS - $START_TIME))
    						fi
    						echo "$ELAPSED_TIME" >$OUTPUT_DIR/readworkflow-iosize-$GET_SIZE-time.log
    					done
    				else
    					echo "ior+dfs is not currently setup for concurrent readers and writers"
    					continue
    				fi
    				echo "Cleaning up daos-posix container"
    				daos pool list-cont --pool=$POOL_UUID | sed -e '1,2d' | awk '{print $1}' | xargs -L 1 -I '{}' sh -c "daos cont destroy --cont={} --pool=$POOL_UUID --force"
    			fi
    
    		done
    	done
    done
done

echo "Destroying previous containers, if any "
daos pool list-cont --pool=$POOL_UUID | sed -e '1,2d' | awk '{print $1}' | xargs -L 1 -I '{}' sh -c "daos cont destroy --cont={} --pool=$POOL_UUID --force"

echo "Generating CSV files"
./parse-result.sh $RESULT_DIR

echo "List of stdout files with error"
find $RESULT_DIR/ -iname 'stdout*.log' | xargs ls -1t | tac | xargs grep --color='auto' -ilE 'error|termination|fail'

echo ""
echo "WRITE TIME"
find $RESULT_DIR/ -iname 'comparewrite*.csv' | xargs tail
echo ""
echo ""
echo "READ TIME"
find $RESULT_DIR/ -iname 'compareread*.csv' | xargs tail
