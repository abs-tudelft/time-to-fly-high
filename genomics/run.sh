#!/bin/bash 

#SBATCH --output=run.out
#SBATCH -t 05:00:00 
#SBATCH -p fat 
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

#echo -n "starting on node master";
#singularity exec /home/tahmad/tahmad/singularity/arrowlatest.simg /home/tahmad/tahmad/neusomatic/test/run_test.sh 0

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
   /usr/bin/ssh ${nodes[$i]} "alias python='/usr/local/bin/python3.8'; singularity exec /home/tahmad/tahmad/singularity/arrowupdated.simg /home/tahmad/tahmad/neusomatic/test/run_test.sh ${i} " &
   #/usr/bin/ssh ${nodes[$i]} "singularity exec /home/tahmad/tahmad/singularity/arrowupdated.simg python /home/tahmad/tahmad/testing/pre/sender.py --proc ${i}  --path /scratch-shared/tahmad/bio_data/FDA/Illumina/HG003/ --nodes 8 " &
done

wait

sleep 5;

echo Done.
