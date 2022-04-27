#!/bin/bash 

#SBATCH --output=run.out
#SBATCH -t 05:00:00 
#SBATCH -p normal 
#SBATCH -N 2

rm nodeslist.txt
nodeset -S  "," -e $SLURM_NODELIST > nodeslist.txt

nodes=($(scontrol show hostname $SLURM_NODELIST))
nnodes=${#nodes[@]}
last=$(( $nnodes - 1 ))
all=$(( $nnodes  ))

alias python='/usr/local/bin/python3.8'

echo opening ssh connections to start master
ssh ${nodes[0]} hostname

i=0

echo opening ssh connections to start the other nodes worker processeses

for i in $( seq 1 $last )
do
    ssh ${nodes[i]} hostname
done

echo starting remote workers
i=0

for i in $( seq 0 $last )
do
   /usr/bin/ssh ${nodes[$i]} "alias python='/usr/local/bin/python3.8'; singularity exec /home/tahmad/tahmad/singularity/arrowupdated.simg script.sh ${i} " &
done

wait

sleep 5;

echo Done.
