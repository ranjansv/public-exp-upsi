#!/bin/bash
#Parsing script to create total averate writetime CSVs

RESULT_DIR=$1

source ${RESULT_DIR}/config.sh

mkdir -p $RESULT_DIR/csv/ 

#Readers execute after writers complete

if [ $BENCH_TYPE == "writer-reader" ]
then
    #Writer Workflow execution time, comparing DAOS KV with ADIOS+POSIX using different aggregation methods
    for NR in $PROCS
    do
        for DATASIZE in $DATA_PER_RANK
        do
            OUTPUT_FILE="${RESULT_DIR}/csv/writeworkflowtime-${NR}ranks-${DATASIZE}mb.csv"
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
		    WRITEWORKFLOW_TIME=`cat $RESULT_DIR/${NR}ranks/${DATASIZE}mb/daos-array/writeworkflow-time.log`
		    echo -n ",$WRITEWORKFLOW_TIME" >> $OUTPUT_FILE
		elif [ $IO_NAME == "adios+daos-posix" ]
		then
                    for ADIOS_XML in $AGGREGATORS 
                    do
                            WRITEWORKFLOW_TIME=`cat $RESULT_DIR/${NR}ranks/${DATASIZE}mb/${IO_NAME}/${ADIOS_XML}/writeworkflow-time.log`
                            echo -n ",$WRITEWORKFLOW_TIME" >> $OUTPUT_FILE
                    done
                    echo -n "," >> $OUTPUT_FILE
		fi
                echo "" >> $OUTPUT_FILE
            done
        done
    done

    #Reader Workflow execution time, comparing DAOS KV with ADIOS+POSIX using different aggregation methods
    for NR in $PROCS
    do
        for DATASIZE in $DATA_PER_RANK
        do
	  for IOSIZE in $READ_IO_SIZE
	  do
            OUTPUT_FILE="${RESULT_DIR}/csv/readworkflowtime-${NR}ranks-${DATASIZE}mb-readsize-${IOSIZE}.csv"
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
		    READWORKFLOW_TIME=`cat $RESULT_DIR/${NR}ranks/${DATASIZE}mb/daos-array/readworkflow-iosize-$IOSIZE-time.log`
		    echo -n ",$READWORKFLOW_TIME" >> $OUTPUT_FILE
		elif [ $IO_NAME == "adios+daos-posix" ]
		then
                    for ADIOS_XML in $AGGREGATORS 
                    do
                            READWORKFLOW_TIME=`cat $RESULT_DIR/${NR}ranks/${DATASIZE}mb/${IO_NAME}/${ADIOS_XML}/readworkflow-iosize-$IOSIZE-time.log`
                            echo -n ",$READWORKFLOW_TIME" >> $OUTPUT_FILE
                    done
                    echo -n "," >> $OUTPUT_FILE
		fi
                echo "" >> $OUTPUT_FILE
            done
          done
        done
    done
elif [ $BENCH_TYPE == "workflow" ]
then
    #Workflow execution time, comparing DAOS KV with ADIOS+POSIX using different aggregation methods
    for NR in $PROCS
    do
        for DATASIZE in $DATA_PER_RANK
        do
            OUTPUT_FILE="${RESULT_DIR}/csv/workflowtime-${NR}ranks-${DATASIZE}mb-readsize-${READ_IO_SIZE}.csv"
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
		elif [ $IO_NAME == "adios+daos-posix" ]
		then
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

#Following is common to both writer-reader and workflow 
    #Writetime
    for NR in $PROCS
    do
        for DATASIZE in $DATA_PER_RANK
        do
            OUTPUT_FILE="${RESULT_DIR}/csv/avgwritetime-${NR}ranks-${DATASIZE}mb.csv"
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
		    WRITE_TIME=`grep inside-barrier $RESULT_DIR/${NR}ranks/${DATASIZE}mb/daos-array/stdout-mpirun-writers.log|awk '{printf "%.2f", $4}'`
		    echo -n ",$WRITE_TIME" >> $OUTPUT_FILE
		elif [ $IO_NAME == "adios+daos-posix" ]
		then
                    for ADIOS_XML in $AGGREGATORS 
                    do
                            WRITE_TIME=`grep inside-barrier $RESULT_DIR/${NR}ranks/${DATASIZE}mb/${IO_NAME}/${ADIOS_XML}/stdout-mpirun-writers.log|awk '{printf "%.2f", $4}'`
                            echo -n ",$WRITE_TIME" >> $OUTPUT_FILE
                    done
                    echo -n "," >> $OUTPUT_FILE
		fi
                echo "" >> $OUTPUT_FILE
            done
        done
    done

    #Readtime
    for NR in $PROCS
    do
        for DATASIZE in $DATA_PER_RANK
        do
	  for IOSIZE in $READ_IO_SIZE
	  do
            OUTPUT_FILE="${RESULT_DIR}/csv/avgreadtime-${NR}ranks-${DATASIZE}mb-readsize-${IOSIZE}.csv"
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
		    READ_TIME=`grep 'read-time' $RESULT_DIR/${NR}ranks/${DATASIZE}mb/daos-array/stdout-mpirun-readers-iosize-$IOSIZE.log|awk '{printf "%.2f", $4}'`
		    echo -n ",$READ_TIME" >> $OUTPUT_FILE
		elif [ $IO_NAME == "adios+daos-posix" ]
		then
                    for ADIOS_XML in $AGGREGATORS 
                    do
                            READ_TIME=`grep ':endstep' $RESULT_DIR/${NR}ranks/${DATASIZE}mb/${IO_NAME}/${ADIOS_XML}/stdout-mpirun-readers-iosize-$IOSIZE.log|awk '{printf "%.2f", $4}'`
                            echo -n ",$READ_TIME" >> $OUTPUT_FILE
                    done
                    echo -n "," >> $OUTPUT_FILE
		fi
                echo "" >> $OUTPUT_FILE
            done
	 done
        done
    done

    #Compare Readtime
    for NR in $PROCS
    do
        for DATASIZE in $DATA_PER_RANK
        do
          OUTPUT_FILE="${RESULT_DIR}/csv/comparereadtime-${NR}ranks-${DATASIZE}mb-readsize.csv"
	  echo -n "io_size" > $OUTPUT_FILE
	  echo  ",$ENGINE" >> $OUTPUT_FILE
          sed -i 's/\s/,/g' $OUTPUT_FILE
	  for IOSIZE in $READ_IO_SIZE
	  do
	    echo -n "$IOSIZE" >> $OUTPUT_FILE
            for IO_NAME in $ENGINE
	    do
		if [ $IO_NAME == "daos-array" ]
		then
		    READ_TIME=`grep 'read-time' $RESULT_DIR/${NR}ranks/${DATASIZE}mb/daos-array/stdout-mpirun-readers-iosize-$IOSIZE.log|awk '{printf "%.2f", $4}'`
		    echo -n ",$READ_TIME" >> $OUTPUT_FILE
		elif [ $IO_NAME == "adios+daos-posix" ]
		then
		     ADIOS_XML=$AGGREGATORS
                     READ_TIME=`grep ':endstep' $RESULT_DIR/${NR}ranks/${DATASIZE}mb/${IO_NAME}/${ADIOS_XML}/stdout-mpirun-readers-iosize-$IOSIZE.log|awk '{printf "%.2f", $4}'`
                     echo -n ",$READ_TIME" >> $OUTPUT_FILE
		elif [ $IO_NAME == "ior+dfs" ]
		then
		     READ_TIME=`grep '^read' $RESULT_DIR/${NR}ranks/${DATASIZE}mb/ior+dfs/stdout-mpirun-readers-iosize-$IOSIZE.log|head -$STEPS|awk 'BEGIN{sum = 0} {sum += $10} END {print sum}'`
                     echo -n ",$READ_TIME" >> $OUTPUT_FILE
		fi
            done
                echo "" >> $OUTPUT_FILE
	 done
        done
    done
