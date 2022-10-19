#!/bin/bash
#Parsing script to create total averate writetime CSVs

RESULT_DIR=$1

source ${RESULT_DIR}/config.sh

mkdir -p $RESULT_DIR/csv/

#Readers execute after writers complete


if [ $BENCH_TYPE == "workflow" ]; then
	#Workflow execution time, comparing DAOS KV with ADIOS+POSIX using different aggregation methods
	for NR in $PROCS; do
		for DATASIZE in $DATA_PER_RANK; do
			OUTPUT_FILE="${RESULT_DIR}/csv/workflowtime-${NR}ranks-${DATASIZE}mb-readsize-${READ_IO_SIZE}.csv"
			echo "io_method,$AGGREGATORS,None" >$OUTPUT_FILE
			sed -i 's/\s/,/g' $OUTPUT_FILE
			for IO_NAME in $ENGINE; do
				echo -n "$IO_NAME" >>$OUTPUT_FILE
				if [ $IO_NAME == "daos-array" ]; then
					for ADIOS_XML in $AGGREGATORS; do
						echo -n "," >>$OUTPUT_FILE
					done
					WORKFLOW_TIME=$(cat $RESULT_DIR/${NR}ranks/${DATASIZE}mb/daos-array/workflow-time.log)
					echo -n ",$WORKFLOW_TIME" >>$OUTPUT_FILE
				elif [ $IO_NAME == "adios+daos-posix" ]; then
					for ADIOS_XML in $AGGREGATORS; do
						WORKFLOW_TIME=$(cat $RESULT_DIR/${NR}ranks/${DATASIZE}mb/${IO_NAME}/${ADIOS_XML}/workflow-time.log)
						echo -n ",$WORKFLOW_TIME" >>$OUTPUT_FILE
					done
					echo -n "," >>$OUTPUT_FILE
				fi
				echo "" >>$OUTPUT_FILE
			done
		done
	done
fi

#Compare Readtime
for NR in $PROCS; do
	for DATASIZE in $DATA_PER_RANK; do
		OUTPUT_FILE="${RESULT_DIR}/csv/comparereadtime-${NR}ranks-${DATASIZE}mb.csv"
		echo -n "io_size" >$OUTPUT_FILE
		echo ",$ENGINE" >>$OUTPUT_FILE
		sed -i 's/\s/,/g' $OUTPUT_FILE
		for IOSIZE in $READ_IO_SIZE; do
			echo -n "$IOSIZE" >>$OUTPUT_FILE
			for IO_NAME in $ENGINE; do
				if [ $IO_NAME == "daos_array_per_adios_var" ]; then
					READ_TIME=$(grep 'read-time' $RESULT_DIR/${NR}ranks/${DATASIZE}mb/${IO_NAME}/stdout-mpirun-readers-iosize-$IOSIZE.log | awk '{printf "%.2f", $4}')
					echo -n ",$READ_TIME" >>$OUTPUT_FILE
				elif [ $IO_NAME == "daos_array_per_rank" ]; then
					READ_TIME=$(grep 'read-time' $RESULT_DIR/${NR}ranks/${DATASIZE}mb/${IO_NAME}/stdout-mpirun-readers-iosize-$IOSIZE.log | awk '{printf "%.2f", $4}')
					echo -n ",$READ_TIME" >>$OUTPUT_FILE
				elif [ $IO_NAME == "adios+daos-posix" ]; then
					ADIOS_XML=$AGGREGATORS
					READ_TIME=$(grep ':endstep' $RESULT_DIR/${NR}ranks/${DATASIZE}mb/${IO_NAME}/${ADIOS_XML}/stdout-mpirun-readers-iosize-$IOSIZE.log | awk '{printf "%.2f", $4}')
					echo -n ",$READ_TIME" >>$OUTPUT_FILE
				elif [ $IO_NAME == "ior+daos-posix" ]; then
					READ_TIME=$(grep '^read' $RESULT_DIR/${NR}ranks/${DATASIZE}mb/ior+daos-posix/stdout-mpirun-readers-iosize-$IOSIZE.log | head -$STEPS | awk 'BEGIN{sum = 0} {sum += $8} END {printf "%.2f", sum}')
					echo -n ",$READ_TIME" >>$OUTPUT_FILE
				elif [ $IO_NAME == "ior+dfs" ]; then
					READ_TIME=$(grep '^read' $RESULT_DIR/${NR}ranks/${DATASIZE}mb/ior+dfs/stdout-mpirun-readers-iosize-$IOSIZE.log | head -$STEPS | awk 'BEGIN{sum = 0} {sum += $8} END {printf "%.2f", sum}')
					echo -n ",$READ_TIME" >>$OUTPUT_FILE
				fi
			done
			echo "" >>$OUTPUT_FILE
		done
	done
done

#Compare Writetime
for NR in $PROCS; do
        for DATASIZE in $DATA_PER_RANK; do
                OUTPUT_FILE="${RESULT_DIR}/csv/comparewritetime-${NR}ranks-${DATASIZE}mb.csv"
                echo -n "datasize" >$OUTPUT_FILE
                echo ",$ENGINE" >>$OUTPUT_FILE
                sed -i 's/\s/,/g' $OUTPUT_FILE
                echo -n $DATASIZE >> $OUTPUT_FILE
                        for IO_NAME in $ENGINE; do
                                if [ $IO_NAME == "daos_array_per_adios_var" ]; then
                                        WRITE_TIME=$(grep 'write-time' $RESULT_DIR/${NR}ranks/${DATASIZE}mb/${IO_NAME}/stdout-mpirun-writers.log | awk '{printf "%.2f", $4}')
                                        echo -n ",$WRITE_TIME" >>$OUTPUT_FILE
                                elif [ $IO_NAME == "daos_array_per_rank" ]; then
                                        WRITE_TIME=$(grep 'write-time' $RESULT_DIR/${NR}ranks/${DATASIZE}mb/${IO_NAME}/stdout-mpirun-writers.log | awk '{printf "%.2f", $4}')
                                        echo -n ",$WRITE_TIME" >>$OUTPUT_FILE
                                elif [ $IO_NAME == "adios+daos-posix" ]; then
                                        ADIOS_XML=$AGGREGATORS
                                        WRITE_TIME=$(grep 'write-time' $RESULT_DIR/${NR}ranks/${DATASIZE}mb/${IO_NAME}/${ADIOS_XML}/stdout-mpirun-writers.log | awk '{printf "%.2f", $4}')
                                        echo -n ",$WRITE_TIME" >>$OUTPUT_FILE
                                elif [ $IO_NAME == "ior+daos-posix" ]; then
                                        WRITE_TIME=$(grep '^write' $RESULT_DIR/${NR}ranks/${DATASIZE}mb/ior+daos-posix/stdout-mpirun-writers.log | head -$STEPS | awk 'BEGIN{sum = 0} {sum += $8} END {printf "%.2f", sum}')
                                        echo -n ",$WRITE_TIME" >>$OUTPUT_FILE
                                elif [ $IO_NAME == "ior+dfs" ]; then
                                        WRITE_TIME=$(grep '^write' $RESULT_DIR/${NR}ranks/${DATASIZE}mb/ior+dfs/stdout-mpirun-writers.log | head -$STEPS | awk 'BEGIN{sum = 0} {sum += $8} END {printf "%.2f", sum}') 
                                        echo -n ",$WRITE_TIME" >>$OUTPUT_FILE
                                fi
                        done
                        echo "" >>$OUTPUT_FILE
        done
done

mkdir -p export-${RESULT_DIR}
cp $RESULT_DIR/config.sh export-${RESULT_DIR}/
cp -r $RESULT_DIR/csv export-${RESULT_DIR}/
