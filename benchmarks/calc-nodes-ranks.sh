req_writer_ranks=$1
ranks_per_node=28

echo "Writer # ranks: ${req_writer_ranks}"

writer_num_nodes=$((($req_writer_ranks + $ranks_per_node - 1)/$ranks_per_node))

echo "writer_num_nodes: $writer_num_nodes"

total_num_nodes=`echo "scale=0; $writer_num_nodes " | bc`

total_slurm_ranks=`echo "scale=0; $total_num_nodes * $ranks_per_node" | bc`


echo "==================="
echo ""
echo "# Slurm Nodes: $total_num_nodes"
echo "# Slurm Ranks: $total_slurm_ranks"
