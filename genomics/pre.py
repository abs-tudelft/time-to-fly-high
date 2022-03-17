#!/usr/bin/python

import multiprocessing
import threading
from multiprocessing import Pool
from multiprocessing.pool import ThreadPool
from threading import Thread #import Barrier, Thread

import numpy as np
import pandas as pd
import pyarrow 
import pyarrow.flight
import pyarrow.plasma as plasma

#import pyspark 

import subprocess
import random
import string
import sys
import os
import glob
import argparse
import shutil
import socket 
import gc
import psutil


import collections
import time
from datetime import datetime

import re
import hashlib
import pysam

#################################################
CHRMS=['1', '110', '120', '130', '140', '150','160', '170', '180', '190', '2', '210', '220', '230', '240', '250', '260', '270', '280', '290', '3', '31', '32', '33', '34','35','36','37', '4', '41', '42', '43', '44','45','46','47', '5', '51', '52', '53', '54','55','56','57', '6', '61', '62', '63', '64','65','66', '7', '71', '72', '73','74','75','76', '8', '81', '82', '83','84','85', '9', '91', '92', '93','94','95', '10', '101', '102', '103','104','105', '11', '111', '112', '113','114','115', '12', '121', '122', '123','124','125', '13', '131', '132','133','134', '14', '141', '142','143', '15', '151', '152','153', '16', '161', '162','163', '17', '171', '172', '18', '181', '182', '19', '191', '20', '201','202', '21', '211', '22', '221', '23', '231', '232', '233','234','235', '24', '241', '25']

#################################################
## getting the hostname by socket.gethostname() method
host = socket.gethostname()
#################################################

def print_mem_usage(event):
    
    mem_usage = "{:.2f}".format(psutil.virtual_memory().available / (1024 ** 3))
    log_msg = "["+ host +" - "+ datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "]: " + event + ", available memory: " + mem_usage + " GB"
    print(log_msg)
    with open(args.path+"mem_profiler.log", "a") as f:
        f.write(log_msg + "\n")

'''
def _arrow_record_batch_dumps(rb):

    #from pyspark.serializers import ArrowSerializer

    #import os
    #os.environ['ARROW_PRE_0_15_IPC_FORMAT'] = '1'

    return bytearray(rb.serialize())

    #return map(bytearray, map(ArrowSerializer().dumps, rb))

def createFromArrowRecordBatchesRDD(self, ardd, schema=None, timezone=None):
    #from pyspark.sql.types import from_arrow_schema
    #from pyspark.sql.dataframe import DataFrame
    #from pyspark.serializers import ArrowSerializer, PickleSerializer, AutoBatchedSerializer

    from pyspark.sql.pandas.types import from_arrow_schema
    from pyspark.sql.dataframe import DataFrame

    # Filter out and cache arrow record batches
    ardd = ardd.filter(lambda x: isinstance(x, pa.RecordBatch)).cache()

    ardd = ardd.map(_arrow_record_batch_dumps)

    #schema = pa.schema([pa.field('c0', pa.int16()),
    #                    pa.field('c1', pa.int32())],
    #                   metadata={b'foo': b'bar'})
    schema = from_arrow_schema(_schema())

    # Create the Spark DataFrame directly from the Arrow data and schema
    jrdd = ardd._to_java_object_rdd()
    jdf = self._jvm.PythonSQLUtils.toDataFrame(jrdd, schema.json(), self._wrapped._jsqlContext)
    df = DataFrame(jdf, self._wrapped)
    df._schema = schema

    return df

def _schema():
    fields = [
        pa.field('beginPoss', pa.int32()),
        pa.field('sam', pa.string())
    ]
    return pa.schema(fields)
'''
#########################################################
def runDeepVariant(inBAM):
    #/scratch-shared/tahmad/bio_data/HG002/HG002_chr15:76493391-86493391.bam
    print(inBAM)

    index_path='/home/tahmad/tahmad/samtools/samtools index -@' + CORES +' '+ inBAM
    os.system(index_path)

    REGION = inBAM.split('_')[-1].replace(".bam", "")
    CHR= inBAM.split('_')[-1].replace(".bam", "").split(':')[0]
    BIN_VERSION="1.1.0"

    OUT=os.path.dirname(inBAM)
    print(REGION)

    DVOUTPUT=OUT+'/dv_output-'+REGION
    WHATSHAPOUTPUT=OUT+'/whatshap_output-'+REGION

    if os.path.exists(DVOUTPUT) and os.path.exists(WHATSHAPOUTPUT):
        shutil.rmtree(DVOUTPUT)
        shutil.rmtree(WHATSHAPOUTPUT)

    os.mkdir(DVOUTPUT)
    os.mkdir(WHATSHAPOUTPUT)

    OUTPUT_DIR=DVOUTPUT+'/quickstart-output-'+REGION

    docker='docker://google/deepvariant:'+BIN_VERSION
    interdir='--intermediate_results_dir='+DVOUTPUT+'/intermediate_results_dir'
    ref_dv='--ref='+REF #/scratch-shared/tahmad/bio_data/GRCh38/GRCh38.fa'
    read='--reads='+inBAM 
    out='--output_vcf='+OUT+'/'+ REGION +'.dv.vcf.gz'
    outg='--output_gvcf='+DVOUTPUT+'/'+ REGION +'.dv.g.vcf.gz'
    region='--regions='+REGION
    #region='--regions=chr20'
    threads='--num_shards='+CORES
 
    #DeepVariant 1-pass
    cmd= ['singularity', 'exec', '-B /usr/lib/locale/', docker, '/opt/deepvariant/bin/run_deepvariant',  '--model_type=PACBIO', ref_dv, read, out, outg, interdir, region, threads]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    process.wait()

    ref=REF #'/scratch-shared/tahmad/bio_data/GRCh38/GRCh38.fa'
    dv_out= OUT+'/'+ REGION +'.dv.vcf.gz'
    outphased=WHATSHAPOUTPUT+'/'+REGION+'.phased.vcf.gz'
    chr_phasing='--chromosome='+CHR
    
    #Whathap phase
    cmd= ['singularity', 'exec', '/home/tahmad/tahmad/singularity/dv.simg', '/home/tahmad/.local/bin/whatshap', 'phase', '--ignore-read-groups', '-o', outphased,  '--reference', ref, chr_phasing, dv_out, inBAM]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    process.wait()

    #Tabix VCF
    tabix='singularity exec /home/tahmad/tahmad/singularity/dv.simg tabix -p vcf '+ outphased
    os.system(tabix)

    outhaplotagged=WHATSHAPOUTPUT+'/'+REGION+'.haplotagged.bam'
    
    #Whathap haplotag
    cmd= ['singularity', 'exec', '/home/tahmad/tahmad/singularity/dv.simg', '/home/tahmad/.local/bin/whatshap', 'haplotag',  '--ignore-read-groups', '-o', outhaplotagged,  '--reference', ref, outphased,  inBAM]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    process.wait()

    #Index haplotagged
    indexhaplotagged='/home/tahmad/tahmad/samtools/samtools index ' + outhaplotagged
    os.system(indexhaplotagged)

    out1='--output_vcf='+OUT+'/'+ REGION +'.haplotagged.vcf.gz'
    outg1='--output_gvcf='+WHATSHAPOUTPUT+'/'+ REGION +'.haplotagged.g.vcf.gz'
    readwh='--reads='+outhaplotagged
    
    #DeepVariant 2-pass
    cmd= ['singularity', 'exec', '-B /usr/lib/locale/',  docker, '/opt/deepvariant/bin/run_deepvariant',  '--model_type=PACBIO', ref_dv, readwh, '--use_hp_information', out1, outg1, interdir, region, threads]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    process.wait()
    
    return 1
#########################################################
def write_back_arrow(pdf, part):

    schema = pyarrow.Schema.from_pandas(pdf, preserve_index=True)
    table = pyarrow.Table.from_pandas(pdf, preserve_index=True)

    name = args.path.split('/')[-2]
    sink = args.path+'output/'+name+'_'+CHRMS[START+part]+'.arrow' #"myfile.arrow"

    # Note new_file creates a RecordBatchFileWriter 
    writer = pyarrow.ipc.new_file(sink, schema)
    writer.write(table)
    writer.close()
    
    #Test back
    #with pyarrow.RecordBatchFileReader(sink) as reader:
    #    batch=reader.get_batch(0)
    #pdf = pyarrow.Table.from_batches([batch]).to_pandas()
    #print(pdf)

    return 1
#########################################################
def write_back_parquet(df, part):
    
    name = args.path.split('/')[-2]
    out = args.path+'output/'+name+'_'+CHRMS[START+part]+'.parquet.gzip'
    
    #df.to_parquet(out, compression='gzip')
    df.write.parquet(out)

    #Test back
    #df=pd.read_parquet(out) 
    #print(df)
    
    return 1
#########################################################
BAM_CMATCH      ='M'
BAM_CINS        ='I'
BAM_CDEL        ='D'
BAM_CREF_SKIP   ='N'
BAM_CSOFT_CLIP  ='S'
BAM_CHARD_CLIP  ='H'
BAM_CPAD        ='P'
BAM_CEQUAL      ='='
BAM_CDIFF       ='X'
BAM_CBACK       ='B'

BAM_CIGAR_STR   ="MIDNSHP=XB"
BAM_CIGAR_SHIFT =4
BAM_CIGAR_MASK  =0xf
BAM_CIGAR_TYPE  =0x3C1A7

CIGAR_REGEX = re.compile("(\d+)([MIDNSHP=XB])")
#########################################################
def qual_score_by_cigar(cigars):
    qpos = 0
    parts = CIGAR_REGEX.findall(cigars)
    for y in range(0, len(parts)):
        op = parts[y][1]
        if op == BAM_CMATCH or \
            op == BAM_CINS or \
            op == BAM_CSOFT_CLIP or \
            op == BAM_CEQUAL or \
            op == BAM_CDIFF:
                   qpos += int(parts[y][0])
    return qpos
#########################################################
def mean_qual(quals):
    qual = [ord(c) for c in quals]
    mqual = (1 - 10 ** (np.mean(qual)/-10))
    return mqual
#########################################################

#########################################################
def short_reads_write_bam_alt(pdf, by, part):


    # importing socket module
    #import socket
    # getting the hostname by socket.gethostname() method
    #hostname = socket.gethostname()

    #********************************************************#
                
    flag_reg=[]
    res_d=[]
    process_next = False
    process_start = False

    index_d=[]
    read_qual_d=[]
    j=0
    # fields to store for each alignment
    #fields = ['index','read_qual']
    #data = {f: list() for f in fields}
        
    len_pdf=len(pdf.index)
    for i in range(0, len_pdf-1):
        if pdf.loc[i+1, 'beginPoss'] == pdf.loc[i, 'beginPoss'] and pdf.loc[i+1, 'pNexts'] == pdf.loc[i, 'pNexts']:
            #if process_next == False and process_start == False:
                # fields to store for each alignment
                #fields = ['index','read_qual']
                #data = {f: list() for f in fields}

            index_d.insert(j, i)
            read_qual_d.insert(j, mean_qual(pdf.loc[i, 'quals']))
            j=j+1
            #data['index'].append(i)
            #data['read_qual'].append(mean_qual(pdf.loc[i, 'quals']))
            process_next = True
        else:
            process_start = True
            res_d.insert(i, pdf.loc[i, 'flags'])

        if process_next == True and process_start == True:
            #sdf = pd.DataFrame(data)
            #rq = sdf["read_qual"]
            read_qual_index = read_qual_d.index( max(read_qual_d) )

            for i in range(0, len(read_qual_d)):
                if not i == read_qual_index:
                    pdf.loc[i, 'flags']= str(int(pdf.loc[i, 'flags']) | 1024)
                #else:
                #    flag_reg.insert(i, pdf.loc[i, 'flags'])
            process_next = False
            process_start = False

            index_d.clear()
            read_qual_d.clear()
            j=0
        
    #********************************************************#

    name = args.path.split('/')[-2] 
    headerpath=sorted(glob.glob(args.path+'parts/'+'*.sam'))
    #headerpath=sorted(glob.glob(args.path+'bams/header.sam'))
    samfile = pysam.AlignmentFile(headerpath[1])
    header = samfile.header
    #print(header)
    samfile.close()
 

    ##/scratch-shared/tahmad/bio_data/FDA/Illumnia/HG003/8/bams/
    sam_name=args.path+'bams/'+name+'_'+CHRMS[START+part]+'.bam'
    print(sam_name)
    #len_pdf=len(pdf.index)

    table=pyarrow.Table.from_pandas(pdf)
    d = table.to_pydict()
    with pysam.AlignmentFile(sam_name, "wb", header=header) as outf:
        for qNames,flags,rIDs,beginPoss, mapQs,cigars,rNextIds,pNexts,tLens,seqs,quals,tagss in zip(d['qNames'], d['flags'], d['rIDs'], d['beginPoss'], d['mapQs'], d['cigars'], d['rNextIds'], d['pNexts'], d['tLens'], d['seqs'], d['quals'], d['tagss']):
        

        

        #for i in range(0, len_pdf-1):
            #if i in duplicates:
            #    res_d.insert(i, sdf.loc[i, 'flags'] | 1024)
            #else:
            #    res_d.insert(i, sdf.loc[i, 'flags'])
            #header = pysam.AlignmentHeader.from_dict(header)
            a = pysam.AlignedSegment(header)
            a.query_name = qNames #pdf.loc[i, 'qNames'] 
            seq=seqs #pdf.loc[i, 'seqs']
            if(seq=='*'):
                a.query_sequence= ""
            else:
                a.query_sequence= seq
            a.flag = int(flags) #pdf.loc[i, 'flags']
            #a.reference_name=pdf.loc[i, 'rIDs']
              
            a.reference_start = int(beginPoss)-1 #pdf.loc[i, 'beginPoss']-1
            #print(a.reference_start)
            a.mapping_quality = int(mapQs) #pdf.loc[i, 'mapQs']
            a.cigarstring = cigars #pdf.loc[i, 'cigars']
            #a.next_reference_name=pdf.loc[i, 'rNextIds']

            pNextsvalue=pNexts #pdf.loc[i, 'pNexts']
            if(pNextsvalue ==0):
                a.next_reference_start= -1 #int(pdf.loc[i, 'pNexts'])
            else:
                a.next_reference_start=int(pNextsvalue)-1

            a.template_length= int(tLens) #pdf.loc[i, 'tLens']
            if(seq=='*'):
                a.query_qualities= ""
            else:
                a.query_qualities = pysam.qualitystring_to_array(quals) #pdf.loc[i, 'quals'])
            #a.tags = (("NM", 1), ("RG", "L1"))
            items = tagss.split('\'') #pdf.loc[i, 'tagss'].split('\'')
            items = items[0].split('\t')
            for item in items[1:]:
                sub = item.split(':')
                a.set_tag(sub[0], sub[-1])
            #a.reference_name=pdf.loc[i, 'rIDs']
            #try:
            #    a.next_reference_name=pdf.loc[i, 'rNextIds']
            #except:
            #    a.next_reference_name="*"
            '''
            rid=pdf.loc[i, 'rIDs']
            if(rid=="chrM"):
                a.reference_id=-1
            elif(rid=="chrX"):
                a.reference_id=22
            elif(rid=="chrY"):
                a.reference_id=23
            else:
                chrno=rid.replace("chr", "")
                a.reference_id = chrno-1
            '''
            a.reference_name=rIDs #pdf.loc[i, 'rIDs']-1
            #a.reference_id=9
            '''
            rnid=pdf.loc[i, 'rNextIds']
            if(rnid=="chrM"):
                a.next_reference_id=-1
            elif(rnid=="chrX"):
                a.next_reference_id=22
            elif(rnid=="chrY"):
                a.next_reference_id=23
            else:
                chrno=rnid.replace("chr", "")
                a.next_reference_id = int(chrno)-1
            '''
            a.next_reference_name = rNextIds #-1
            #a.next_reference_id = 10
            outf.write(a)

    index_path='/home/tahmad/tahmad/samtools/samtools index ' + sam_name
    #os.system(index_path)

    return 1

#########################################################

#########################################################

def sam_operations_pairs(pdf):
 
    res_d=[]
    process_next = False
    process_start = False

    index_d=[]
    read_qual_d=[]
    j=0
    # fields to store for each alignment
    #fields = ['index','read_qual']
    #data = {f: list() for f in fields}
    
    len_pdf=len(pdf.index)
    for i in range(0, len_pdf-1):
        if pdf.loc[i+1, 'beginPoss'] == pdf.loc[i, 'beginPoss'] and pdf.loc[i+1, 'pNexts'] == pdf.loc[i, 'pNexts']: 
            #if process_next == False and process_start == False:
                # fields to store for each alignment
                #fields = ['index','read_qual']
                #data = {f: list() for f in fields}
             
            index_d.insert(j, i)
            read_qual_d.insert(j, mean_qual(pdf.loc[i, 'quals']))
            j=j+1
            #data['index'].append(i)
            #data['read_qual'].append(mean_qual(pdf.loc[i, 'quals']))
            process_next = True
        else: 
            process_start = True
            res_d.insert(i, pdf.loc[i, 'flags'])

        if process_next == True and process_start == True:
            #sdf = pd.DataFrame(data)
            #rq = sdf["read_qual"]
            read_qual_index = read_qual_d.index( max(read_qual_d) ) 

            for i in range(0, len(read_qual_d)):
                if i == read_qual_index:
                    #res_d.insert(sdf.loc[i, 'index'], int(pdf.loc[i, 'flags']))
                    res_d.insert(index_d[i], pdf.loc[i, 'flags']) 
                else: 
                    #res_d.insert(sdf.loc[i, 'index'], int(pdf.loc[i, 'flags']) | 1024)  
                    res_d.insert(index_d[i], pdf.loc[i, 'flags'] | 1024)
            process_next = False 
            process_start = False
 
            index_d.clear()
            read_qual_d.clear()
            j=0

             
    res_d.insert(len_pdf, pdf.loc[len_pdf-1, 'flags'])
    
    return  pd.DataFrame({'flags': res_d})

#########################################################

def sam_md_operations_pairs(pdf, index):

    res_d=[]
    process_next = False
    process_start = False

    index_d=[]
    read_qual_d=[]
    j=0
    # fields to store for each alignment
    #fields = ['index','read_qual']
    #data = {f: list() for f in fields}

    len_pdf=len(pdf.index)
    for i in range(0, len_pdf-1):
        if pdf.loc[i+1, 'beginPoss'] == pdf.loc[i, 'beginPoss'] and pdf.loc[i+1, 'pNexts'] == pdf.loc[i, 'pNexts']:
            #if process_next == False and process_start == False:
                # fields to store for each alignment
                #fields = ['index','read_qual']
                #data = {f: list() for f in fields}

            index_d.insert(j, i)
            read_qual_d.insert(j, mean_qual(pdf.loc[i, 'quals']))
            j=j+1
            #data['index'].append(i)
            #data['read_qual'].append(mean_qual(pdf.loc[i, 'quals']))
            process_next = True
        else:
            process_start = True
            res_d.insert(i, pdf.loc[i, 'flags'])

        if process_next == True and process_start == True:
            #sdf = pd.DataFrame(data)
            #rq = sdf["read_qual"]
            read_qual_index = read_qual_d.index( max(read_qual_d) )

            for i in range(0, len(read_qual_d)):
                if not i == read_qual_index:
                    pdf.loc[i, 'flags']= pdf.loc[i, 'flags'] | 1024
                  
            process_next = False
            process_start = False

            index_d.clear()
            read_qual_d.clear()
            j=0
    #write_back_arrow(pdf, index)
    write_back_parquet(pdf, index)
    return  1 #pdf #pd.DataFrame({"foo1": np.random.randn(5)}) #pd.DataFrame(res_d, columns='flags')

def write_back_bam(pdf, part):
    #with pyarrow.RecordBatchFileReader('/scratch-shared/tahmad/bio_data/FDA/Illumina/HG003/arrow/gcn16/1_chr1:1-24895641.arrow') as reader:
    #    batch=reader.get_batch(0)
    #pdf = pyarrow.Table.from_batches([batch]).to_pandas()
    #print(pdf)
    #pdf["beginPoss"] = pd.to_numeric(pd["beginPoss"])
    #pdf.sort_values(by=["beginPoss"], ascending=True, inplace=True, ignore_index=True)
    #print(pdf)
    #pdf.info(verbose=True)

    name = args.path.split('/')[-2]
    headerpath=sorted(glob.glob(args.path+'bams/'+name+'_header_'+'*.sam'))
    samfile = pysam.AlignmentFile('/scratch-shared/tahmad/bio_data/FDA/Illumina/HG003/8/HG003_1.part_001_tcn1565.bullx.sam') #headerpath[0])
    header = samfile.header
    #print(header)
    samfile.close()

    ##/scratch-shared/tahmad/bio_data/FDA/Illumnia/HG003/8/bams/
    sam_name=args.path+'bams/'+name+'_'+CHRMS[START+part]+'.bam'
    len_pdf=len(pdf.index)
    #print("Start pdf_operations on ", host, "  count=", len(pdf.index), "  index=", part)
    with pysam.AlignmentFile(sam_name, "wb", header=header) as outf:

        for i in range(0, len_pdf-1):
            #if i in duplicates:
            #    res_d.insert(i, sdf.loc[i, 'flags'] | 1024)
            #else:
            #    res_d.insert(i, sdf.loc[i, 'flags'])
            #header = pysam.AlignmentHeader.from_dict(header)
            a = pysam.AlignedSegment()#(header)
            a.query_name = pdf.loc[i, 'qNames']

            seq=pdf.loc[i, 'seqs']
            if(seq=='*'):
                a.query_sequence= ""
            else:
                a.query_sequence= seq

            a.flag = int(pdf.loc[i, 'flags'])
            #a.reference_name=pdf.loc[i, 'rIDs']

            a.reference_start = pdf.loc[i, 'beginPoss']-1
            #print(a.reference_start)
            a.mapping_quality = int(pdf.loc[i, 'mapQs'])
            a.cigarstring = pdf.loc[i, 'cigars']
            #a.next_reference_name=pdf.loc[i, 'rNextIds']

            pNextsvalue=int(pdf.loc[i, 'pNexts'])
            if(pNextsvalue ==0):
                a.next_reference_start= -1 #int(pdf.loc[i, 'pNexts'])
            else:
                a.next_reference_start=pNextsvalue

            a.template_length= int(pdf.loc[i, 'tLens'])
            if(seq=='*'):
                a.query_qualities= ""
            else:
                a.query_qualities = pysam.qualitystring_to_array(pdf.loc[i, 'quals'])
            #a.tags = (("NM", 1), ("RG", "L1"))
            items = pdf.loc[i, 'tagss'].split('\'')
            #print(items)
            for item in items[1:]:
                sub = item.split(':')
                a.set_tag(sub[0], sub[-1])
               
            rid=pdf.loc[i, 'rIDs']
            #if(rid=="chrM"):
            #    a.reference_id=-1
            #elif(rid=="chrX"):
            #    a.reference_id=22
            #elif(rid=="chrY"):
            #    a.reference_id=23
            #else:
            #    chrno="1"#rid.replace("chr", "")
            a.reference_id = int(rid)-1
            
            a.next_reference_id = -1

            outf.write(a)
    return 1 
'''                            
def long_reads_write_bam(df, by, part):

    def sam_operations(pdf):

        # importing socket module
        #import socket
        # getting the hostname by socket.gethostname() method
        #hostname = socket.gethostname()

        name = args.path.split('/')[-2]
        headerpath=sorted(glob.glob(args.path+'bams/'+name+'_header_'+'*.sam'))
        samfile = pysam.AlignmentFile(headerpath[0])
        header = samfile.header
        #print(header)
        samfile.close()
        
        ##/scratch-shared/tahmad/bio_data/FDA/Illumnia/HG003/8/bams/
        sam_name=args.path+'bams/'+name+'_'+part+'.bam'
        len_pdf=len(pdf.index)
        with pysam.AlignmentFile(sam_name, "wb", header=header) as outf:

            for i in range(0, len_pdf-1):
                #if i in duplicates:
                #    res_d.insert(i, sdf.loc[i, 'flags'] | 1024)
                #else:
                #    res_d.insert(i, sdf.loc[i, 'flags'])
                #header = pysam.AlignmentHeader.from_dict(header)
                a = pysam.AlignedSegment()#(header)
                a.query_name = pdf.loc[i, 'qNames'] 

                seq=pdf.loc[i, 'seqs']
                if(seq=='*'):
                    a.query_sequence= ""
                else:
                    a.query_sequence= seq

                a.flag = int(pdf.loc[i, 'flags'])
                #a.reference_name=pdf.loc[i, 'rIDs']
                
                a.reference_start = int(pdf.loc[i, 'beginPoss'])-1
                #print(a.reference_start)
                a.mapping_quality = int(pdf.loc[i, 'mapQs'])
                a.cigarstring = pdf.loc[i, 'cigars']
                #a.next_reference_name=pdf.loc[i, 'rNextIds']

                pNextsvalue=int(pdf.loc[i, 'pNexts'])
                if(pNextsvalue ==0):
                    a.next_reference_start= -1 #int(pdf.loc[i, 'pNexts'])
                else:
                    a.next_reference_start=pNextsvalue

                a.template_length= int(pdf.loc[i, 'tLens'])
                if(seq=='*'):
                    a.query_qualities= ""
                else:
                    a.query_qualities = pysam.qualitystring_to_array(pdf.loc[i, 'quals'])
                #a.tags = (("NM", 1), ("RG", "L1"))
                items = pdf.loc[i, 'tagss'].split('\'')
                #print(items)
                for item in items[1:]:
                    sub = item.split(':')
                    a.set_tag(sub[0], sub[-1])

                #a.reference_name=pdf.loc[i, 'rIDs']
                #try:
                #    a.next_reference_name=pdf.loc[i, 'rNextIds']
                #except:
                #    a.next_reference_name="*"
                rid=pdf.loc[i, 'rIDs']
                if(rid=="chrM"):
                    a.reference_id=-1
                elif(rid=="chrX"):
                    a.reference_id=22
                elif(rid=="chrY"):
                    a.reference_id=23
                else:
                    chrno=rid.replace("chr", "")
                    a.reference_id = int(chrno)-1
                
                a.next_reference_id = -1

                outf.write(a)

        index_path='/home/tahmad/tahmad/samtools/samtools index ' + sam_name
        #os.system(index_path)

        return  pdf[['flags']] #pdf.loc[:, 'flags'] #pd.DataFrame({'flags': res_d})

    return df.groupby(by).applyInPandas(sam_operations, schema="flags string")

#########################################################
def arrow_data_collection(path):
    with pa.RecordBatchFileReader(path) as reader:
        batch = reader.get_batch(0)
    #print(batch.to_pandas())
    return batch

def process_output(itern):
    arrow_file_list = sorted(glob.glob(args.path+'arrow/'+CHRMS[itern]+"_" + "*.arrow"))
    ardd = spark.sparkContext.parallelize(arrow_file_list, len(arrow_file_list)).map(arrow_data_collection)
    df = spark.createFromArrowRecordBatchesRDD(ardd).orderBy('beginPoss', ascending=True).coalesce(1)
    df = df.select(split(df.sam,"\t")).rdd.flatMap(lambda x: x).toDF(schema=["qNames","flags","rIDs","beginPoss", "mapQs", "cigars", "rNextIds", "pNexts", "tLens", "seqs", "quals", "tagss"])
    long_reads_write_bam(df, by="rIDs", part=arrow_file_list[0].split('_')[-2]).show()
    df.unpersist(True)    
    return 1

#######################################################
def get_arrow_batches(path):
    with pa.RecordBatchFileReader(path) as reader:
        batch=reader.get_batch(0)
    return batch#.to_pandas()

def local_sort(index):
    arrow_file_list = sorted(glob.glob(args.path+'arrow/'+index+"_" + "*.arrow"))
    part=arrow_file_list[0].split('_')[-2]
    batches = [get_arrow_batches(arrow_file) for arrow_file in arrow_file_list]
    pdf=pa.Table.from_batches(batches).to_pandas()
    #print(pdf)
    #pdf = pd.concat(pdfs)
    #pdf.sort_values(by=['beginPoss'], inplace=True)
    #df = pd.DataFrame(pdf.sam.str.split('\t').tolist(), columns = ["qNames","flags","rIDs","beginPoss", "mapQs", "cigars", "rNextIds", "pNexts", "tLens", "seqs", "quals", "tagss"])
    pdf.sort_values(by=['beginPoss'], ascending=True, inplace=True) 
    df = pd.DataFrame(pdf.sam.str.split('\t').tolist(), columns = ["qNames","flags","rIDs","beginPoss", "mapQs", "cigars", "rNextIds", "pNexts", "tLens", "seqs", "quals", "tagss"])
    pdf_operations(df, part)    
    #print(df.beginPoss)
     
    return 1

def pandas_sorting(itern):
    ## importing socket module
    import socket
    ## getting the hostname by socket.gethostname() method
    hostname = socket.gethostname()
    print(hostname)

    print(itern)    
    #parts=CHRMS[itern[0]*itern[1]:itern[0]*itern[1]+itern[1]]
    parts=CHRMS[itern[0]:itern[0]+itern[1]]
    print(parts)
    #pool = Pool(initializer=None, initargs=(), processes=len(parts))    
    #ids = pool.map(local_sort, parts)
    p = ThreadPool(len(parts))
    p.map(local_sort, parts)
    return 1
#######################################################
'''
def run_Minimap2(fqfile):
    print(fqfile)

    ## importing socket module
    import socket
    ## getting the hostname by socket.gethostname() method
    hostname = socket.gethostname()

    name=args.path.split('/')[-2]
    plfm=args.path.split('/')[-3]
    rg='@RG\\tID:'+name+'\\tSM:sample\\tPL:'+plfm+'\\tLB:sample\\tPU:lane' 
    print(rg)
    out = args.path+'bams/'+name+'_header_'+hostname+'.sam' 
    cmd = ['/home/tahmad/tahmad/testing/minimap2/minimap2', '-a',  '-k 19',  '-O 5,56',  '-E 4,1',  '-B 5',  '-z 400,50',  '-r 2k',  '--eqx',  '--secondary=no', '-R', rg, '-t', CORES, REF, fqfile, '-o', out]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    process.wait()

    return hostname

def streaming_Minimap2(path):
    #/scratch-shared/tahmad/bio_data/FDA/HG002/
    file_name = path.split('/')[-2]
    #parts=path.split('/')[-1]
    cmd = '/home/tahmad/tahmad/seqkit split2 --threads=32 ' + args.path + file_name +'.fastq -p '+NODES+' -O '+ args.path + 'parts' +' -f'
    os.system(cmd)
    return 1

def process_Minimap2(path):
    time.sleep(5)
    #/scratch-shared/tahmad/bio_data/FDA/HG002/
    file_name = path.split('/')[-2]
    file_names = sorted(glob.glob(args.path+'parts/'+file_name+".part_"+ "*.fastq"))
    frdd = spark.sparkContext.parallelize(file_names, len(file_names))
    frdd = frdd.map(run_Minimap2).collect()
    return 1

#########################################################
def local_sort(index):
    #print("Started concat_tables() operation on ", host)
    print_mem_usage("Started concat_tables() operation")
    table = pyarrow.concat_tables(tables[index])
    #del tables[index]
    #print("Started table.to_pandas() operation on ", host)
    print_mem_usage("Started table.to_pandas() operation")
    pdf = table.to_pandas()
    del table
    print_mem_usage("Finished table.to_pandas() operation")
    #print("Finished table.to_pandas() operation on ", host, "  count=", len(pdf.index), "  index=", index)
    pdf.sort_values(by=['beginPoss'], ascending=True, inplace=True, ignore_index=True)
    #print("Finished pandas sort operation on ", host, "  count=", len(pdf.index), "  index=", index)
    print_mem_usage("Finished pdf.sort_values operation")
    pdf = pd.DataFrame(pdf.sam.str.split('\t').tolist(), columns = ["qNames","flags","rIDs","beginPoss", "mapQs", "cigars", "rNextIds", "pNexts", "tLens", "seqs", "quals", "tagss"])
    short_reads_write_bam_alt(pdf, by="rIDs", part=index)
    #df.unpersist()
    print_mem_usage("Finished md and BAM writing operation")
    
    #write_back_arrow(pdf, index)
    #sam_md_operations_pairs(pdf, index)
    #md_df = pdf.groupby("rIDs").applyInPandas(sam_operations_pairs, schema="flags integer")
    #print(md_df)
    #print_mem_usage("Finished write_back to arrow operation")
    
    #write_back_parquet(pdf, index)
    #print_mem_usage("Finished write_back to parquet operation")
    
    #sam_operations_pairs(pdf)
    #write_back_bam(pdf, index)
    #print_mem_usage("Finished BAM writing operation")

    return 1
#########################################################

def run_BWA(path,num):

    time.sleep(5)
    
    ## importing socket module
    import socket
    ## getting the hostname by socket.gethostname() method
    hostname = socket.gethostname()
   
    name=args.path.split('/')[-2]
    plfm=args.path.split('/')[-3]

    if(int(num)<10):
        index=num.zfill(2)
    else:
        index=num
    fqfile1=args.path+'parts/'+name+'_1.part_0'+index+'.fastq.gz'
    fqfile2=args.path+'parts/'+name+'_2.part_0'+index+'.fastq.gz'
    print(fqfile1)
 
    out = fqfile1.replace(".fastq", "")+'_'+hostname+'.sam'
    rg='@RG\\tID:'+name+'\\tSM:sample\\tPL:'+plfm+'\\tLB:sample\\tPU:lane'
    
    cmd = ['/home/tahmad/tahmad/testing/pre/bwa', 'mem', '-R', rg, '-t', CORES, REF, fqfile1, fqfile2, '-o', out]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    process.wait()

    return hostname

def streaming_BWA(path):
    #/scratch-shared/tahmad/bio_data/FDA/HG002/
    file_name = path.split('/')[-2]
    cmd = '/home/tahmad/tahmad/seqkit split2 --threads='+ CORES +' -1 '+ path + file_name +'_1.fastq.gz -2 '+ path + file_name  +'_2.fastq.gz -p '+ args.nodes  +' -O '+ path + 'parts/ -f'
    os.system(cmd)
    return 1

#########################################################
def hostname(x):
    ## importing socket module
    import socket
    ## getting the hostname by socket.gethostname() method
    hostname = socket.gethostname()
    ## getting the IP address using socket.gethostbyname() method
    ip_address = socket.gethostbyname(hostname)
    return [hostname,ip_address]
#########################################################
def get_flight_server():
    tls_certificates = []
    scheme = "grpc+tcp"
    if args.tls:
        scheme = "grpc+tls"
        with open(args.tls[0], "rb") as cert_file:
            tls_cert_chain = cert_file.read()
        with open(args.tls[1], "rb") as key_file:
            tls_private_key = key_file.read()
        tls_certificates.append((tls_cert_chain, tls_private_key))
    host="0.0.0.0"
    port=32108
    location = "{}://{}:{}".format(scheme, host, port)

    return FlightServer(args.host, location, tls_certificates=tls_certificates, verify_client=args.verify_client)

class FlightServer(pyarrow.flight.FlightServerBase):
    def __init__(self, host="localhost", location=None,
                 tls_certificates=None, verify_client=False,
                 root_certificates=None, auth_handler=None):
        super(FlightServer, self).__init__(
            location, auth_handler, tls_certificates, verify_client,
            root_certificates)
        self.flights = {}
        self.host = host
        self.tls_certificates = tls_certificates     
   
    @classmethod
    def descriptor_to_key(self, descriptor):
        return (descriptor.descriptor_type.value, descriptor.command,
                tuple(descriptor.path or tuple()))

    def _make_flight_info(self, key, descriptor, table):
        if self.tls_certificates:
            location = pyarrow.flight.Location.for_grpc_tls(
                self.host, self.port)
        else:
            location = pyarrow.flight.Location.for_grpc_tcp(
                self.host, self.port)
        endpoints = [pyarrow.flight.FlightEndpoint(repr(key), [location]), ]

        mock_sink = pyarrow.MockOutputStream()
        stream_writer = pyarrow.RecordBatchStreamWriter(
            mock_sink, table.schema)
        stream_writer.write_table(table)
        stream_writer.close()
        data_size = mock_sink.size()

        return pyarrow.flight.FlightInfo(table.schema,
                                         descriptor, endpoints,
                                         table.num_rows, data_size)

    def list_flights(self, context, criteria):
        for key, table in self.flights.items():
            if key[1] is not None:
                descriptor = \
                    pyarrow.flight.FlightDescriptor.for_command(key[1])
            else:
                descriptor = pyarrow.flight.FlightDescriptor.for_path(*key[2])

            yield self._make_flight_info(key, descriptor, table)

    def get_flight_info(self, context, descriptor):
        key = FlightServer.descriptor_to_key(descriptor)
        if key in self.flights:
            table = self.flights[key]
            return self._make_flight_info(key, descriptor, table)
        raise KeyError('Flight not found.')

    def do_put(self, context, descriptor, reader, writer):
        key = FlightServer.descriptor_to_key(descriptor)
        print(descriptor.path[0].decode("utf-8") )
        #print(key)

        #with open(log_file, "a") as f:
        #    f.write("[" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "]: ")
        #    f.write(key + ' : '+ descriptor.path[0].decode("utf-8") + 
        #            " : received Arrow RecordBatch.\n")

        self.flights[key] = reader.read_all()
        #print(self.flights[key])
        #print("server do put")
        i=0
        for itern in  range(START, END, 1):
            if(re.search('/'+CHRMS[itern]+'_chr', descriptor.path[0].decode("utf-8"))):
                tables[i].append(self.flights[key])

                #if(args.proc=="3" and CHRMS[itern] =='55'):
                #    print("Inside loop, ", itern)
                #    pdf = self.flights[key].to_pandas()
                #    print(pdf)

                global total
                total+=1
                break
            i+=1

        if(total==PARTS):
            print("shutdown command..!")

            #with open(log_file, "a") as f:
            #    f.write("[" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "]: ")
            #    f.write("Received " + total +
            #            " Arrow RecordBatches : shutdown command..!.\n")

            buf = pyarrow.allocate_buffer(0)
            action = pyarrow.flight.Action("shutdown", buf)
            for result in FlightServer.do_action(self, context, action):
                print("Got result", result.body.to_pybytes())
       
    def do_get(self, context, ticket):
        key = ast.literal_eval(ticket.ticket.decode())
        if key not in self.flights:
            return None
        print("server do get")
        return pyarrow.flight.RecordBatchStream(self.flights[key])

    def list_actions(self, context):
        return [
            ("clear", "Clear the stored flights."),
            ("shutdown", "Shut down this server."),
        ]

    def do_action(self, context, action):
        if action.type == "clear":
            raise NotImplementedError(
                "{} is not implemented.".format(action.type))
        elif action.type == "healthcheck":
            pass
        elif action.type == "shutdown":
            yield pyarrow.flight.Result(pyarrow.py_buffer(b'Shutdown!'))
            # Shut down on background thread to avoid blocking current
            # request
            threading.Thread(target=self._shutdown).start()
        else:
            raise KeyError("Unknown action {!r}".format(action.type))

    def _shutdown(self):
        """Shut down after a delay."""
        print("Server is shutting down...")
        time.sleep(2)
        self.shutdown()
#########################################################
class ExcPassThroughThread(Thread):
    """Wrapper around `threading.Thread` that propagates exceptions."""

    def __init__(self, target, *args):
        Thread.__init__(self, target=target, *args)
        self.exc = None

    def run(self):
        """Method representing the thread's activity.
        You may override this method in a subclass. The standard run() method
        invokes the callable object passed to the object's constructor as the
        target argument, if any, with sequential and keyword arguments taken
        from the args and kwargs arguments, respectively.
        """
        try:
            Thread.run(self)
        except BaseException as e:
            self.exc = e

    def join(self, timeout=None):
        """Wait until the thread terminates.
        This blocks the calling thread until the thread whose join() method is
        called terminates -- either normally or through an unhandled exception
        or until the optional timeout occurs.
        When the timeout argument is present and not None, it should be a
        floating point number specifying a timeout for the operation in seconds
        (or fractions thereof). As join() always returns None, you must call
        is_alive() after join() to decide whether a timeout happened -- if the
        thread is still alive, the join() call timed out.
        When the timeout argument is not present or None, the operation will
        block until the thread terminates.
        A thread can be join()ed many times.
        join() raises a RuntimeError if an attempt is made to join the current
        thread as that would cause a deadlock. It is also an error to join() a
        thread before it has been started and attempts to do so raises the same
        exception.
        """
        super(ExcPassThroughThread, self).join(timeout)
        if self.exc:
            raise self.exc
#########################################################
parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
requiredNamed = parser.add_argument_group('Required arguments')

parser.add_argument("-bam",  "--bam",  help="Testing mode.", type=str, default='both')

###################
parser.add_argument("--host", type=str, default="localhost", help="Address or hostname to listen on")
parser.add_argument("--port", type=int, default=5005, help="Port number to listen on")
parser.add_argument("--tls", nargs=2, default=None, metavar=('CERTFILE', 'KEYFILE'), help="Enable transport-level security")
parser.add_argument("--verify_client", type=bool, default=False, help="enable mutual TLS and verify the client if True")
###################


requiredNamed.add_argument("-proc",  "--proc",  help="Process number", required=True)
requiredNamed.add_argument("-part",  "--part",  help="Part number of pipeline", required=True)
requiredNamed.add_argument("-ref",  "--ref",  help="Reference genome file with .fai index", required=True)
requiredNamed.add_argument("-nodes",  "--nodes",  help="Number of nodes", required=True)
requiredNamed.add_argument("-path",  "--path",  help="Input FASTQ path", required=True)
requiredNamed.add_argument("-cores",  "--cores",  help="Number od cores on each node", required=True)
requiredNamed.add_argument("-aligner",  "--aligner",  help="Aligner (BWA or Minimap2)", required=True)

args = parser.parse_args()

CORES=args.cores
NODES=args.nodes
REF=args.ref


#from args
INDEX=int(args.proc)

PARTS=128
NODES=int(args.nodes)
N=int(PARTS)/int(NODES)

START=(INDEX-1)*int(N)
END=((INDEX-1)*int(N))+int(N)

total=0

tables =[[] for i in range(int(N))]
#########################################################

if __name__ == '__main__':

    #host_name = socket.gethostname().strip(".bullx")
    #log_file = host_name + "_receiver.log"

    print_mem_usage("Start of the profiler")

    #spark = SparkSession \
    #    .builder \
    #    .appName("Variant calling workflow") \
    #    .getOrCreate()

    # Enable Arrow-based columnar data transfers
    #spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    #outIDs = spark.sparkContext.parallelize([0,1,2,3,4,5,6,7],8).map(lambda x: hostname(x)).collect()
    #print(outIDs)
    #outIDs = spark.sparkContext.parallelize([0,1,2,3,4,5,6,7],8).map(lambda x: hostname(x)).collect()
    #print(outIDs)

    #with pyarrow.RecordBatchFileReader('/scratch-shared/tahmad/bio_data/ERR194147/ERR194147/arrow/tcn1569/101_chr10:22299570-44599139.arrow') as reader:
    #    batch=reader.get_batch(0)
    #pdf = pyarrow.Table.from_batches([batch]).to_pandas()
    #print(pdf)
    ##pd["beginPoss"] = pd.to_numeric(pd["beginPoss"])
    #pdf.sort_values(by=['beginPoss'], ascending=True, inplace=True, ignore_index=True)
    #print(pdf)
    #pdf = pd.DataFrame(pdf.sam.str.split('\t').tolist(), columns = ["qNames","flags","rIDs","beginPoss", "mapQs", "cigars", "rNextIds", "pNexts", "tLens", "seqs", "quals", "tagss"])
    #print(pdf)
    #short_reads_write_bam_alt(pdf, by="rIDs", part=0)
    
    #pdf.unpersist()
    #print_mem_usage("writing Arrow")
    #write_back_arrow(pdf, 0)
    #print_mem_usage("writing Parquet")
    #write_back_parquet(pdf, 0)
    #write_back_bam(pdf, 0)
    #pdf_operations(1, 0)
    #df=sam_operations_pairs(pd)
    #md_df = pdf.groupby("rIDs").applyInPandas(sam_operations_pairs, schema="flags integer")
    #print(md_df)
    ####################################
    #start = time.time()
    #################################### 

    if(args.part=="1" and args.aligner=="BWA" and args.proc=='0'):

        ####################################
        start = time.time()
        #################################### 

        #path="/scratch-shared/tahmad/bio_data/FDA/Illumina/HG003/"
        OUTDIR=args.path
        ARROWOUT=OUTDIR+'arrow'
        BAMOUT=OUTDIR+'bams'
        OUT=OUTDIR+'output'
    

        if os.path.exists(OUT):
            shutil.rmtree(OUT)
        try:
            os.mkdir(OUT)
        except OSError:
            print ("Creation of the directory %s failed" % OUT)
        else:
            print ("Successfully created the directory %s " % OUT)

        #if os.path.exists(ARROWOUT):
        #    shutil.rmtree(ARROWOUT)
        if os.path.exists(BAMOUT):
            shutil.rmtree(BAMOUT)
        #try:
        #    os.mkdir(ARROWOUT)
        #except OSError:
        #    print ("Creation of the directory %s failed" % ARROWOUT)
        #else:
        #    print ("Successfully created the directory %s " % ARROWOUT)

        try:
            os.mkdir(BAMOUT)
        except OSError:
            print ("Creation of the directory %s failed" % BAMOUT)
        else:
            print ("Successfully created the directory %s " % BAMOUT)

        streaming_BWA(args.path)
        
        ####################################
        stop = time.time()
        print('Streaming took {} seconds.'
              .format(stop - start))
        ####################################

    elif(args.part=="1" and args.aligner=="BWA" and not args.proc=='0'):

        
        ####################################
        start = time.time()
        #################################### 

        OUTDIR=args.path
        hname=host.replace('.bullx','')
        ARROWOUTHOST=OUTDIR+'arrow/'+hname
        if os.path.exists(ARROWOUTHOST):
            shutil.rmtree(ARROWOUTHOST) 
        try:
            os.mkdir(ARROWOUTHOST)
        except OSError:
            print ("Creation of the directory %s failed" % ARROWOUTHOST)
        else:
            print ("Successfully created the directory %s " % ARROWOUTHOST)

        #BWA aligner
        run_BWA(args.path, args.proc)

        #Arrow Flight Sender
        #cmd='python3 sender.py'+' --proc '+ args.proc +' --path '+ args.path
        #subprocess.run(cmd, shell=True)
        
        '''
        if(args.aligner=="Minimap2"):
        
            job1 = multiprocessing.Process(target=streaming_Minimap2, args=(args.path,))
            job1.start()
            job2 = multiprocessing.Process(target=process_Minimap2, args=(args.path,))
            job2.start()
            # Wait for both jobs to finish
            job1.join()
            job2.join()
        
            #process_Minimap2(args.path)
        
        elif(args.aligner=="BWA"):
            run_BWA(args.path, args.proc)
        
            job1 = multiprocessing.Process(target=streaming_BWA, args=(args.path,))
            job1.start()
            job2 = multiprocessing.Process(target=process_BWA, args=(args.path,))
            job2.start()
            # Wait for both jobs to finish
            job1.join()
            job2.join()
        '''
        ####################################
        stop = time.time()
        print('Aligner took {} seconds.'
              .format(stop - start))
        ####################################
        
    elif(args.part=="2" and args.aligner=="BWA" and not args.proc=='0'):
         
        #SparkSession.createFromArrowRecordBatchesRDD = createFromArrowRecordBatchesRDD

        ####################################
        start = time.time()
        ####################################

        PARTS= 128

        #for x in range(0, PARTS, int(NODES)+8):
        #    p = ThreadPool(int(NODES)+8)
        #    p.map(process_output, range(x, int(NODES)+8+x, 1))

        #p = ThreadPool(3)
        #p.map(process_output, range(3)

        #sort_list=list(zip(range(0, PARTS, int(PARTS/int(NODES))) , [int(PARTS/int(NODES))]*int(NODES)))
        #print(sort_list)
        #brdd = spark.sparkContext.parallelize(sort_list, len(sort_list))
        #brdd = brdd.map(pandas_sorting).collect()

        #ids = pandas_sorting([0, 16])            
        #print(ids)
        #process_output(0)
        
        print_mem_usage("SERVER - Before starting Arrow Flight communication.")
        #Arrow Flight Receiver
        get_flight_server().serve()
        print_mem_usage("SERVER - After finishing  Arrow Flight communication.")
        
        ####################################
        stop = time.time()
        print('Flight communication took {} seconds.'
              .format(stop - start))
        ####################################



        ####################################
        start = time.time()
        ####################################

        #Merging tables, Creating pandas dfs and Sorting
        print("Started ThreadPool process on ", host)
        '''
        if(args.proc=="3"):
            print("Entering Pandas on ", host, " index=9")
            table = pyarrow.concat_tables(tables[9])
            print("pyarrow.concat_tables done on ", host, " index=9")
            pdf = table.to_pandas()
            print("Pandas on ", host, " index=9")
            print(pdf)
        elif (args.proc=="5"):
            print("Entering Pandas on ", host, " index=13")
            table = pyarrow.concat_tables(tables[13])
            print("pyarrow.concat_tables done on ", host, " index=13")
            pdf = table.to_pandas()
            print("Pandas on ", host, " index=13")
            print(pdf)
        elif (args.proc=="8"):
            print("Entering Pandas on ", host, " index=7")
            table = pyarrow.concat_tables(tables[7])
            print("pyarrow.concat_tables done on ", host, " index=7")
            pdf = table.to_pandas()
            print("Pandas on ", host, " index=7")
            print(pdf)
        '''
        #table = pyarrow.concat_tables(tables[0])
        #pdf = table.to_pandas()
        #print(pdf)
        #pdf.sort_values(by=['beginPoss'], ascending=True, inplace=True)
        #print(pdf)
        #pdf_operations(pdf, 0)        
         
        #threads = []
        #for i in range(16):#int(N)):
        #    t = threading.Thread(target=local_sort, args=(i,))
        #    threads.append(t)
        #    t.start()
        #for t in threads:
        #    t.join()
        
        #local_sort(0)
        #b = Barrier(16, timeout=15)
        #threads = [ExcPassThroughThread(target=local_sort, i) for i in range(int(N))]
        #for t in threads:
        #    t.start()
        #for t in threads:
        #    t.join()
                 
        #print_mem_usage("After Pandas operations.")
        
        p = ThreadPool(16)
        p.map(local_sort, range(16))
        print_mem_usage("After Pandas operations.")
        ####################################
        stop = time.time()
        print('BAM output took {} seconds.'
              .format(stop - start))
        ####################################
        
    elif(args.part=="3"):        
        
        files_bam = sorted(glob.glob(BAMOUT+'*_chr'+'*.bam'), key=os.path.getsize)
        print(files_bam)
        #for x in range(0, PARTS, int(NODES)):

        #    files_bam_part=files_bam[x:x+int(NODES)]
        #    brdd = spark.sparkContext.parallelize(files_bam_part, len(files_bam_part))
        #    brdd = brdd.map(runDeepVariant).collect()
        
        #files_bam_part=sorted(glob.glob(BAMOUT+'*_chr20'+'*.bam'), key=os.path.getsize)
        #brdd = spark.sparkContext.parallelize(files_bam_part, len(files_bam_part))
        #brdd = brdd.map(runDeepVariant).collect()

        #runDeepVariant(args.path+'bams/HG003_chr1:1-24895641.bam')

    #spark.stop()
