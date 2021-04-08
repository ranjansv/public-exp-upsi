#!/bin/bash
#Print perf stats for a specific run

DIR=$1
RANK=$2
DATASIZE=$3
ENGINE=$4

find results/$DIR/ -iname 'std*.log'|grep ${RANK}ranks|grep ${DATASIZE}mb|grep $ENGINE|xargs -L 1 tail -30
