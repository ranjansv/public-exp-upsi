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
git branch --show-current > git branch --show-current
git log --format="%H" -n 1 >> $RESULT_DIR/git.log

#Build source
cd build
make
cd ..

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

if [ $WRITER_NUMA == $READER_NUMA ]; 
then   
	echo "Both writers and readers are placed on the same socket"
	exit 1
fi

#if [ $WRITER_NUMA == "local" ] && [ $READER_NUMA == "local" ];
#then   
#       reader_lastcpu=55
#       reader_firstcpu=$(( $reader_lastcpu - $thr + 1))
#else   
#       reader_lastcpu=$(( $reader_firstcpu + ${thr} - 1))
#fi

for NR in $PROCS
do
    writer_lastcpu=$(( $writer_firstcpu + ${NR} - 1))
    mkdir -p $RESULT_DIR/${NR}ranks/console
    for ENG_TYPE in $ENGINE
    do
        for DATASIZE in $TOTAL_DATA_PER_RANK
        do
            echo "Processing ${NR}ranks, ${ENG_TYPE}writers, ${DATASIZE}mb"
            #Choose PROCS and STEPS so that global array size is a whole numebr
	    GLOBAL_ARRAY_SIZE=`echo "scale=0; $DATASIZE * ($NR/$STEPS)" | bc`
	    echo "global array size: $GLOBAL_ARRAY_SIZE"

	    rm -rf /mnt/pmem1/output.bp &> /dev/null


	    OUTPUT_DIR="$RESULT_DIR/${NR}ranks/${ENG_TYPE}writers/${DATASIZE}mb"
            mkdir -p $OUTPUT_DIR
            perf stat -d -d -d numactl -m 1 mpirun --cpu-set ${writer_firstcpu}-${writer_lastcpu}  -np $NR --bind-to core --mca btl tcp,self build/writer $ENG_TYPE $GLOBAL_ARRAY_SIZE $STEPS &>> $RESULT_DIR/${NR}ranks/console/stdout-${NR}ranks-${ENG_TYPE}writers-${DATASIZE}mb.log
            #perf stat -d -d -d mpirun --cpu-set ${writer_firstcpu}-${writer_lastcpu}  -np $NR --bind-to core --mca btl tcp,self build/writer $ENG_TYPE $GLOBAL_ARRAY_SIZE $STEPS &>> $RESULT_DIR/${NR}ranks/console/stdout-${NR}ranks-${ENG_TYPE}writers-${DATASIZE}mb.log
            mv writer*.log $OUTPUT_DIR/
        done
    done
done


./parse-result.sh $RESULT_DIR

echo "CSV directory:"
echo "$RESULT_DIR/csv"

cat $RESULT_DIR/csv/*.csv



