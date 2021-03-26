#!/bin/bash
#Parsing script to create total averate writetime CSVs

RESULT_DIR=$1

source ${RESULT_DIR}/config.sh

for NR in $PROCS
do
    #Setup outfile for recording average total write time   
    mkdir -p $RESULT_DIR/csv/ 
    OUTPUT_FILE="${RESULT_DIR}/csv/writetime-${NR}ranks.csv"
    echo "totaldataperank,$ENGINE" > $OUTPUT_FILE
    sed -i 's/\s/,/g' $OUTPUT_FILE

    for DATASIZE in $TOTAL_DATA_PER_RANK
    do
	    DATASIZE_IN_GB=`echo "scale=1; $DATASIZE/1024" | bc`
        echo -n "$DATASIZE_IN_GB" >> $OUTPUT_FILE

        for ENG_TYPE in $ENGINE
        do
             AVG_TOTAL_WRITE_TIME=`ls -1 $RESULT_DIR/${NR}ranks/${ENG_TYPE}writers/${DATASIZE}mb/writer*.log|xargs -L 1 tail -1|awk -F '\t' '{print $4}'| awk 'BEGIN{sum=0;} {sum+=$1} END{printf "%.f\n", sum/NR/1000;}'`
            echo -n ",$AVG_TOTAL_WRITE_TIME" >> $OUTPUT_FILE
        done
        echo "" >> $OUTPUT_FILE
    done
done
