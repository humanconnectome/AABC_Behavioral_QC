import pandas as pd
#import yaml
#import ccf
from ccf.box import LifespanBox
#import requests
import re
import collections
from functions import *
#import functions
from config import *
#import subprocess
#import os
#import sys
from datetime import date

## get configuration files
outp="/Users/petralenzini/work/Behavioral/AABC/AABC_Behavioral_QC/AABC_Behavioral_QC/tmp/"

config = LoadSettings("/Users/petralenzini/work/Behavioral/AABC/AABC_Behavioral_QC/AABC_Behavioral_QC/config.yml")
secret=pd.read_csv(config['config_files']['secrets'])
box = LifespanBox(cache=outp)
scratch=205351313707

# NOW FOR IntraDB. ############################################################################
def importITK(siteabbrev='WU'):
    outdir = "/Users/petralenzini/work/Behavioral/AABC/AABC_Behavioral_QC/AABC_Behavioral_QC"
    command="for i in `ls /ceph/intradb/archive/AABC_WU_ITK/arc001`; do echo -n $i >> /home/plenzini/tools/catTLBX/intradb_list.txt; tree -L 1 /ceph/intradb/archive/AABC_WU_ITK/arc001/$i/SCANS | wc >> /home/plenzini/tools/catTLBX/intradb_list.txt; done"
    run_ssh_cmd('plenzini@login3.chpc.wustl.edu',
                        command).stdout.read()

    #run_ssh_cmd('plenzini@login3.chpc.wustl.edu',
    #                    'find /ceph/intradb/archive/AABC_'+siteabbrev+'_ITK/ -type f  z! \( -name "*Scores*" -o -name "*Narrow*" -o -name "*Regist*z" -o -name "*catalog*" \) > /home/plenzini/tools/catTLBX/datalist.csv').stdout.read()
    #return

##FIRST THE ITK DATA FILES
itk4=importITK(siteabbrev='WU')
itk1=importITK(siteabbrev='MGH')
itk3=importITK(siteabbrev='UMN')
itk2=importITK(siteabbrev='UCLA')

stg=importSTG()

#send this to temp
itk=pd.concat([tlbxraw1,tlbxraw2,tlbxraw3,tlbxraw4])

