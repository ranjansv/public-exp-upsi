#!/bin/bash

declare -a read_write_ratio=("1" "2" "4")
for ratio in "${read_write_ratio[@]}"
do
	echo "Executing bench-run with reader-writer ratio: $ratio"
	echo "=================================================="
	sed -i "s/READ_WRITE.*/READ_WRITE_RATIO=\"${ratio}\"/g" config/writer-fulltest-config.sh 
	START_TIME=$SECONDS
	./bench-run.sh config/writer-fulltest-config.sh
	ELAPSED_TIME=$(($SECONDS - $START_TIME))
	echo "COMPLETED bench-run with reader-writer ratio: $ratio"
	echo "Elapsed time: $ELAPSED_TIME seconds"
done
