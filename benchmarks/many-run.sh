#!/bin/bash

TEST=$1

for i in {1..4}
do
	echo "Executing $i bench-run"
	echo "=================================================="
	./bench-run.sh $TEST
done
