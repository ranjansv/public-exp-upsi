#!/bin/bash
#Parsing script to create total averate writetime CSVs

RESULT_DIR=$1

source ${RESULT_DIR}/config.sh

#Generate writetime csvs
#for NR in $PROCS
#do
#    #Setup outfile for recording average total write time   
#    mkdir -p $RESULT_DIR/csv/ 
#    OUTPUT_FILE="${RESULT_DIR}/csv/writetime-${NR}ranks.csv"
#    echo "totaldataperank,$ENGINE" > $OUTPUT_FILE
#    sed -i 's/\s/,/g' $OUTPUT_FILE
#
#    for DATASIZE in $DATA_PER_RANK
#    do
#	DATASIZE_IN_GB=`echo "scale=1; $DATASIZE/1024" | bc`
#        echo -n "$DATASIZE_IN_GB" >> $OUTPUT_FILE
#
#        for IO_NAME in $ENGINE
#        do
#             AVG_TOTAL_WRITE_TIME=`ls -1 $RESULT_DIR/${NR}ranks/${IO_NAME}/${DATASIZE}mb/writer*.log|xargs -L 1 tail -1|awk -F '\t' '{print $4}'| awk 'BEGIN{sum=0;} {sum+=$1} END{printf "%.f\n", sum/NR/1000;}'`
#            echo -n ",$AVG_TOTAL_WRITE_TIME" >> $OUTPUT_FILE
#        done
#        echo "" >> $OUTPUT_FILE
#    done
#done

if [ $BENCH_TYPE == "writer" ]
then

    for NR in $PROCS
    do
        #Setup outfile for recording average total write time   
        mkdir -p $RESULT_DIR/csv/ 
    
        for DATASIZE in $DATA_PER_RANK
        do
            TOTAL_DATA_SIZE=`echo "scale=0; $DATASIZE * ($NR) * $STEPS" | bc`
            if [ $TOTAL_DATA_SIZE != $PRESET_TOTAL_DATA_SIZE ]
            then
                continue
            fi
            OUTPUT_FILE="${RESULT_DIR}/csv/workflowtime-${NR}ranks-${DATASIZE}mb.csv"
            echo "io_method,$AGGREGATORS,None" > $OUTPUT_FILE
            sed -i 's/\s/,/g' $OUTPUT_FILE
            for IO_NAME in $ENGINE
	    do
	        echo -n "$IO_NAME" >> $OUTPUT_FILE
		if [ $IO_NAME == "daos-array" ]
		then
                    for ADIOS_XML in $AGGREGATORS 
                    do
                        echo -n "," >> $OUTPUT_FILE
		    done
		    WORKFLOW_TIME=`cat $RESULT_DIR/${NR}ranks/${DATASIZE}mb/daos-array/workflow-time.log`
		    echo -n ",$WORKFLOW_TIME" >> $OUTPUT_FILE
		else
                    for ADIOS_XML in $AGGREGATORS 
                    do
                            WORKFLOW_TIME=`cat $RESULT_DIR/${NR}ranks/${DATASIZE}mb/${IO_NAME}/${ADIOS_XML}/workflow-time.log`
                            echo -n ",$WORKFLOW_TIME" >> $OUTPUT_FILE
                    done
                    echo -n "," >> $OUTPUT_FILE
		fi
                echo "" >> $OUTPUT_FILE
            done
        done
    done
 fi
