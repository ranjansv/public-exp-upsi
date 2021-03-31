BENCH_TYPE="workflow"
ENGINE="bp4 sst"
TOTAL_DATA_PER_RANK="512"
STEPS=8
#Ratio of reader to writer ranks is 1:4
PROCS="8"
WRITER_NUMA="local"
READER_NUMA="remote"
