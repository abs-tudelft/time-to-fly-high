#!/bin/bash

cat /proc/sys/kernel/hostname
echo $1

alias python='/usr/local/bin/python3.8'

if [[ $1 -eq 0 ]]
then
  echo "Master node. > Runing Streaming app."
  python3.8 pre.py --part 1 --proc $1  --ref /home/tahmad/hawk/reference/GRCh38_no_alt_analysis_set.fasta  --path /scratch-shared/tahmad/bio_data/ERR194147/ERR194147/  --nodes 2 --cores 24 --aligner BWA 
else
  echo "Worker nodes. > Runing aligner app."
  python3.8 pre.py --part 1 --proc $1  --ref /home/tahmad/hawk/reference/GRCh38_no_alt_analysis_set.fasta  --path /scratch-shared/tahmad/bio_data/ERR194147/ERR194147/  --nodes 2 --cores 22 --aligner BWA 

  echo "Worker nodes. > Runing Arrow Flight and Sorting apps."
  python3.8 pre.py --part 2 --proc $1  --ref /home/tahmad/hawk/reference/GRCh38_no_alt_analysis_set.fasta  --path /scratch-shared/tahmad/bio_data/ERR194147/ERR194147/  --nodes 2 --cores 24 --aligner BWA & python3.8 sender.py --proc $1  --path /scratch-shared/tahmad/bio_data/ERR194147/ERR194147/ --nodes 2

  #echo "Worker nodes. > Runing Variant Calling apps."
  #python pre.py --part 3 --proc $1  --ref /scratch-shared/tahmad/bio_data/GRCh38/GRCh38.fa  --path /scratch-shared/tahmad/bio_data/FDA/Test/HG003/  --nodes 2 --cores 24 --aligner BWA
fi
