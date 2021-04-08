#!/bin/bash
#Print write times of bp4 vs bp4+sst in a given result directory
DIR=$1
RANK=$2
DATASIZE=$3

echo "bp4writes"
echo ""|awk '{print "total\tbp4\tsst"}'
find results/$DIR/${RANK}ranks/bp4writers/${DATASIZE}mb/  -iname 'writer*.log'|xargs -L 1 tail -1|awk '{printf "%.f\t%.f\t%.f\n",$4/1000,$5/1000,$6/1000}'
echo ""
echo "bp4+sstwriters"
echo ""|awk '{print "total\tbp4\tsst"}'
find results/$DIR/${RANK}ranks/bp4+sstwriters/${DATASIZE}mb/  -iname 'writer*.log'|xargs -L 1 tail -1|awk '{printf "%.f\t%.f\t%.f\n",$4/1000,$5/1000,$6/1000}'
