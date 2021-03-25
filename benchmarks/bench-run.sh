#!/bin/bash
#Benchmark script

CONFIG_FILE=$1

#Load config
source ${CONFIG_FILE}

#Create timestamped output directory
TIMESTAMP=`echo $(date +%Y-%m-%d-%H:%M:%S)`
RESULT_DIR="results/$TIMESTAMP"
mkdir -p $RESULT_DIR

mount|grep dax > $RESULT_DIR/fs-type.log

#Copy configs and xml to outputdir
cp ${CONFIG_FILE} $RESULT_DIR/config.sh
cp ./adios2.xml $RESULT_DIR

if [ $WRITER_NUMA == "local" ];
then
        writer_firstcpu=28
else
        writer_firstcpu=0
fi

if [ $READER_NUMA == "local" ];
then
        reader_firstcpu=28
else
        reader_firstcpu=0
fi

writer_lastcpu=$(( $writer_firstcpu + ${thr} - 1))

if [ $WRITER_NUMA == "local" ] && [ $READER_NUMA == "local" ];
then   
       reader_lastcpu=55
       reader_firstcpu=$(( $reader_lastcpu - $thr + 1))
else   
       reader_lastcpu=$(( $reader_firstcpu + ${thr} - 1))
fi

for DATASIZE in $GLOBAL_ARRAYSIZE_GB
do
    mkdir -p $RESULT_DIR/${DATASIZE}gb/console/
    for ENG_TYPE in $ENGINE
    do
        for NR in $PROCS
        do
            echo "Processing ${DATASIZE}gb, ${ENG_TYPE}writers, ${NR} ranks"
	    OUTPUT_DIR="$RESULT_DIR/${DATASIZE}gb/${ENG_TYPE}writers/${NR}ranks/"
            mkdir -p $OUTPUT_DIR
            mpirun --cpu-set ${writer_firstcpu}-${writer_lastcpu}  -np $NR --bind-to core --mca btl tcp,self build/writer $ENG_TYPE $DATASIZE $STEPS &>> $RESULT_DIR/${DATASIZE}gb/console/stdout-${ENG_TYPE}writers-${DATASIZE}gb-${NR}ranks.log
            mv writer*.log $OUTPUT_DIR/
        done
    done
done


./parse-result.sh $RESULT_DIR

echo "CSV directory:"
echo "$RESULT_DIR/csv"



