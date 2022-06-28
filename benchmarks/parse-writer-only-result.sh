#!/bin/bash
#Parsing script to create total averate writetime CSVs

RESULT_DIR=$1

source ${RESULT_DIR}/config.sh

mkdir -p $RESULT_DIR/csv/

#Compare Writetime
for NR in $PROCS; do
        for DATASIZE in $DATA_PER_RANK; do
                OUTPUT_FILE="${RESULT_DIR}/csv/comparewritetime-${NR}ranks-${DATASIZE}mb.csv"
                echo -n "datasize" >$OUTPUT_FILE
                #echo ",$ENGINE" >>$OUTPUT_FILE
                #sed -i 's/\s/,/g' $OUTPUT_FILE
                for IO_NAME in $ENGINE; do
		    if [ $IO_NAME == "adios+daos-posix" ]; then
		        for ADIOS_XML in $AGGREGATORS; do
			    echo -n ",$IO_NAME:$ADIOS_XML" >> $OUTPUT_FILE
			done
		    else
		        echo -n ",$IO_NAME" >>$OUTPUT_FILE
		    fi
		done
                echo "" >> $OUTPUT_FILE
                echo -n $DATASIZE >> $OUTPUT_FILE
                        for IO_NAME in $ENGINE; do
                                if [ $IO_NAME == "daos-array" ]; then
                                        WRITE_TIME=$(grep 'write-time-outside-barrier' $RESULT_DIR/${NR}ranks/${DATASIZE}mb/daos-array/stdout-mpirun-writers.log | awk '{printf "%.2f", $4}')
                                        echo -n ",$WRITE_TIME" >>$OUTPUT_FILE
                                elif [ $IO_NAME == "adios+daos-posix" ]; then
		                        for ADIOS_XML in $AGGREGATORS; do
                                            WRITE_TIME=$(grep 'write-time-outside-barrier' $RESULT_DIR/${NR}ranks/${DATASIZE}mb/${IO_NAME}/${ADIOS_XML}/stdout-mpirun-writers.log | awk '{printf "%.2f", $4}')
                                            echo -n ",$WRITE_TIME" >>$OUTPUT_FILE
					done
                                elif [ $IO_NAME == "ior+daos-posix" ]; then
                                        WRITE_TIME=$(grep '^write' $RESULT_DIR/${NR}ranks/${DATASIZE}mb/ior+daos-posix/stdout-mpirun-writers.log | head -$STEPS | awk 'BEGIN{sum = 0} {sum += $10} END {printf "%.2f", sum}')
                                        echo -n ",$WRITE_TIME" >>$OUTPUT_FILE
                                elif [ $IO_NAME == "ior+dfs" ]; then
                                        WRITE_TIME=$(grep '^write' $RESULT_DIR/${NR}ranks/${DATASIZE}mb/ior+dfs/stdout-mpirun-writers.log | head -$STEPS | awk 'BEGIN{sum = 0} {sum += $10} END {printf "%.2f", sum}') 
                                        echo -n ",$WRITE_TIME" >>$OUTPUT_FILE
                                fi
                        done
                        echo "" >>$OUTPUT_FILE
        done
done
