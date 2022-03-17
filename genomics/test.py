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



def pdf_operations(pdf, part):
    with pyarrow.RecordBatchFileReader('/scratch-shared/tahmad/bio_data/FDA/Illumina/HG003/arrow/gcn16/1_chr1:1-24895641.arrow') as reader:
        batch=reader.get_batch(0)
    pdf = pyarrow.Table.from_batches([batch]).to_pandas()
    #print(pdf)
    #pdf["beginPoss"] = pd.to_numeric(pd["beginPoss"])
    pdf.sort_values(by=["beginPoss"], ascending=True, inplace=True, ignore_index=True)
    #print(pdf)
    #pdf.info(verbose=True)
   
    name = "HG003"#args.path.split('/')[-2]
    #headerpath=sorted(glob.glob(args.path+'bams/'+name+'_header_'+'*.sam'))
    samfile = pysam.AlignmentFile('/scratch-shared/tahmad/bio_data/FDA/Illumina/HG003/8/HG003_1.part_001_tcn1565.bullx.sam') #headerpath[0])
    header = samfile.header
    #print(header)
    samfile.close()

    path='/scratch-shared/tahmad/bio_data/FDA/Illumina/HG003/'
    sam_name=path+'bams/'+name+'_'+CHRMS[part]+'.bam'
    len_pdf=len(pdf.index)
    #print("Start pdf_operations on ", host, "  count=", len(pdf.index), "  index=", part)
    with pysam.AlignmentFile(sam_name, "wb", header=header) as outf:

        for index, read in pdf.iterrows(): #for i in range(0, len_pdf-1):
            #if i in duplicates:
            #    res_d.insert(i, sdf.loc[i, 'flags'] | 1024)
            #else:
            #    res_d.insert(i, sdf.loc[i, 'flags'])
            #header = pysam.AlignmentHeader.from_dict(header)
            a = pysam.AlignedSegment()#(header)
            a.query_name = read['qNames'] #pdf.loc[i, 'qNames']

            seq=read['seqs']#pdf.loc[i, 'seqs']
            if(seq=='*'):
                a.query_sequence= ""
            else:
                a.query_sequence= seq

            a.flag = read['flags'] #int(pdf.loc[i, 'flags'])
            #a.reference_name=pdf.loc[i, 'rIDs']

            a.reference_start = read['beginPoss']-1 #pdf.loc[i, 'beginPoss']-1
            #print(a.reference_start)
            a.mapping_quality = read['mapQs'] #int(pdf.loc[i, 'mapQs'])
            a.cigarstring = read['cigars'] #pdf.loc[i, 'cigars']
            #a.next_reference_name=pdf.loc[i, 'rNextIds']

            pNextsvalue=read['pNexts'] #int(pdf.loc[i, 'pNexts'])
            if(pNextsvalue ==0):
                a.next_reference_start= -1 #int(pdf.loc[i, 'pNexts'])
            else:
                a.next_reference_start=pNextsvalue

            a.template_length= read['tLens'] #int(pdf.loc[i, 'tLens'])
            if(seq=='*'):
                a.query_qualities= ""
            else:
                a.query_qualities = pysam.qualitystring_to_array(read['quals'])#pdf.loc[i, 'quals'])
            #a.tags = (("NM", 1), ("RG", "L1"))
            items = read['tagss'].split('\'') #pdf.loc[i, 'tagss'].split('\'')
            #print(items)
            for item in items[1:]:
                sub = item.split(':')
                a.set_tag(sub[0], sub[-1])

            rid=read['rIDs'] #pdf.loc[i, 'rIDs']
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

if __name__ == '__main__':
    pdf_operations(1,0)
