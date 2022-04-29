Git clone repo:

    git clone https://github.com/abs-tudelft/time-to-fly-high.git 
    cd time-to-fly-high/genomics

Create Apache Arrow based [Singularity](https://sylabs.io/guides/3.0/user-guide/installation.html) container from [singularity](https://github.com/abs-tudelft/time-to-fly-high/blob/main/genomics/singularity.def) definition file or [install](https://arrow.apache.org/install/) Apache Arrow directly on your system:

    sudo singularity build --sandbox bionic singularity.def #Local system
    sudo singularity build arrowupdated.simg bionic
    
    OR
    
    singularity build --fakeroot arrowupdated.simg singularity.def #HPC system
    
Change the path for `singularity exec /home/tahmad/tahmad/singularity/arrowupdated.simg` in `run.sh` file accordingly. 

Download data:

    mkdir -p reference

    FTPDIR=ftp://ftp.ncbi.nlm.nih.gov/genomes/all/GCA/000/001/405/GCA_000001405.15_GRCh38/seqs_for_alignment_pipelines.ucsc_ids
    curl ${FTPDIR}/GCA_000001405.15_GRCh38_no_alt_analysis_set.fna.gz | gunzip > reference/GRCh38_no_alt_analysis_set.fasta
    curl ${FTPDIR}/GCA_000001405.15_GRCh38_no_alt_analysis_set.fna.fai > reference/GRCh38_no_alt_analysis_set.fasta.fai

    mkdir ERR194147
    cd ERR194147

    wget ftp://ftp.sra.ebi.ac.uk/vol1/fastq/ERR194/ERR194147/ERR194147_1.fastq.gz
    wget ftp://ftp.sra.ebi.ac.uk/vol1/fastq/ERR194/ERR194147/ERR194147_2.fastq.gz
    cd ..

Index reference:

    git clone https://github.com/lh3/bwa.git
    cd bwa; make
    ./bwa index reference/GRCh38_no_alt_analysis_set.fasta

ArrowSAM based BWA-MEM:

    wget https://github.com/tahashmi/bwa/releases/download/0.02/bwa-sam.tar.gz
    tar -zxvf bwa-sam.tar.gz
    cd bwa-sam
    make clean
    make 
    
Running pipeline on a SLURM cluster:

Make sure to change the sbatch script with nodes configuration in `run.sh` file.

Set properties for `--ref` (reference), `--path` (input FASTQ, i.e, ERR194147), `--nodes`, `--cores` correctly in `script.sh` file. Finally run:

    sbatch run.sh
