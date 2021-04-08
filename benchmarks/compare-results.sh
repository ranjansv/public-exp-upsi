#!/bin/bash

#Results after removed memory footprint and membind to NUMA 1
#declare -a results=("2021-03-27-02:14:27"
#"2021-03-27-02:04:34"
#"2021-03-27-01:57:33"
#"2021-03-27-01:49:09"
#"2021-03-27-01:40:39")

#Results after stopping background while
#declare -a results=("2021-03-27-17:28:38"
#"2021-03-27-17:14:51"
#"2021-03-27-17:00:57"
#"2021-03-27-16:45:43"
#"2021-03-27-16:34:18")

#Results - Measuring bp4 and sst separately

#declare -a results=("2021-03-28-00:35:30"
#"2021-03-28-00:20:17"
#"2021-03-28-00:04:00"
#"2021-03-27-23:50:58"
#"2021-03-27-23:37:05")

#Results - Perf + numabind to 1 + measure bp4 and sst separately
#declare -a results=("2021-03-28-05:52:49"
#"2021-03-28-05:38:53"
#"2021-03-28-05:23:42"
#"2021-03-28-05:09:14"
#"2021-03-28-04:52:03")

#Results - Reorder SST and later BP4
#declare -a results=("2021-03-29-08:03:29"
#"2021-03-29-07:48:02"
#"2021-03-29-07:33:06"
#"2021-03-29-07:17:18"
#"2021-03-29-07:00:01")

#Results = Fixed writer_lastcpu in bench_run.sh
declare -a results=("2021-03-30-22:09:33"
"2021-03-30-22:01:56"
"2021-03-30-21:54:19"
"2021-03-30-21:46:42"
"2021-03-30-21:38:03")

#Results - Perf + numabind to 1 + measure bp4 and sst separately with reduced data per rank
# 256mb, 512mb and 1024mb
#declare -a results=("2021-03-28-07:46:39"
#"2021-03-28-07:37:51"
#"2021-03-28-07:29:25"
#"2021-03-28-07:21:22"
#"2021-03-28-07:12:42")

#declare total_data_per_rank=(".2" ".5" "1.0")
declare total_data_per_rank=(".5" "1.0" "2.0")

for nr in 8 16 24 
do
    for datasize in "${total_data_per_rank[@]}"
    do
	echo "Comparing $nr ranks, ${datasize}gb results"
        for dir in "${results[@]}"
        do
		#echo $nr $datasize $dir
		echo -n "$dir "
		grep -F "$datasize" results/$dir/csv/writetime-${nr}ranks.csv
        done
    done
done
