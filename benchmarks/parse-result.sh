#!/bin/bash
#Parsing script to create total averate writetime CSVs

RESULT_DIR=$1

source ${RESULT_DIR}/config.sh

#Generate writetime csvs
for NR in $PROCS
do
    #Setup outfile for recording average total write time   
    mkdir -p $RESULT_DIR/csv/ 
    OUTPUT_FILE="${RESULT_DIR}/csv/writetime-${NR}ranks.csv"
    echo "totaldataperank,$ENGINE" > $OUTPUT_FILE
    sed -i 's/\s/,/g' $OUTPUT_FILE

    for DATASIZE in $DATA_PER_RANK
    do
	DATASIZE_IN_GB=`echo "scale=1; $DATASIZE/1024" | bc`
        echo -n "$DATASIZE_IN_GB" >> $OUTPUT_FILE

        for IO_NAME in $ENGINE
        do
             AVG_TOTAL_WRITE_TIME=`ls -1 $RESULT_DIR/${NR}ranks/${IO_NAME}/${DATASIZE}mb/writer*.log|xargs -L 1 tail -1|awk -F '\t' '{print $4}'| awk 'BEGIN{sum=0;} {sum+=$1} END{printf "%.f\n", sum/NR/1000;}'`
            echo -n ",$AVG_TOTAL_WRITE_TIME" >> $OUTPUT_FILE
        done
        echo "" >> $OUTPUT_FILE
    done
done

#Generate readtime csvs
for NR in $PROCS
do
    #Setup outfile for recording average total read time   
    mkdir -p $RESULT_DIR/csv/
    NR_READERS=`echo "scale=0; $NR/$READ_WRITE_RATIO" | bc`
    
    OUTPUT_FILE="${RESULT_DIR}/csv/readtime-${NR_READERS}ranks.csv"
    echo "totaldataperank,$ENGINE" > $OUTPUT_FILE
    sed -i 's/\s/,/g' $OUTPUT_FILE

    for DATASIZE in $DATA_PER_RANK
    do
	#Multiply by 4 to compute datasize per rank of the reader
	DATASIZE_IN_GB=`echo "scale=1; $DATASIZE * $READ_WRITE_RATIO/1024" | bc`
        echo -n "$DATASIZE_IN_GB" >> $OUTPUT_FILE

        for IO_NAME in $ENGINE
        do
            AVG_TOTAL_READ_TIME=`ls -1 $RESULT_DIR/${NR}ranks/${IO_NAME}/${DATASIZE}mb/reader*.log|xargs -L 1 tail -1|awk -F '\t' '{print $3}'| awk 'BEGIN{sum=0;} {sum+=$1} END{printf "%.f\n", sum/NR/1000;}'`
            echo -n ",$AVG_TOTAL_READ_TIME" >> $OUTPUT_FILE
        done
        echo "" >> $OUTPUT_FILE
    done
done


if [ $BENCH_TYPE == "workflow" ]
then

    for NR in $PROCS
    do
        #Setup outfile for recording average total write time   
        mkdir -p $RESULT_DIR/csv/ 
        OUTPUT_FILE="${RESULT_DIR}/csv/workflowtime-${NR}ranks.csv"
        echo "global_array_size,$ENGINE" > $OUTPUT_FILE
        sed -i 's/\s/,/g' $OUTPUT_FILE
    
        for DATASIZE in $DATA_PER_RANK
        do
		GLOBAL_ARRAY_SIZE_GB=`echo "scale=1; ($DATASIZE * $NR/1024)" | bc`
            echo -n "$GLOBAL_ARRAY_SIZE_GB" >> $OUTPUT_FILE
    
            for IO_NAME in $ENGINE
            do
                WORKFLOW_TIME=`cat $RESULT_DIR/${NR}ranks/${IO_NAME}/${DATASIZE}mb/workflow-time.log`
                echo -n ",$WORKFLOW_TIME" >> $OUTPUT_FILE
            done
            echo "" >> $OUTPUT_FILE
        done
    done
    fi
