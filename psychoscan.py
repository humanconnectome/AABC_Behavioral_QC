import pandas as pd
#import yaml
#import ccf
from ccf.box import LifespanBox
#import requests
import re
import collections
#from functions import *
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


#part 1 1 scan box
#can't figure out why some of the B files aren't being grabbed in the file listing from Box
#studyshort='WU'
folderqueue=['WU','MGH','UMN','UCLA']

#scan Box
anydata=pd.DataFrame()
for studyshort in folderqueue:
    folder = config['NonQBox']['Psychopy'][studyshort]
    #dag = config['Redcap']['datasources']['aabcarms'][studyshort]['dag']
    #sitenum = config['Redcap']['datasources']['aabcarms'][studyshort]['sitenum']
    filelist=box.list_of_files([folder])
    db=pd.DataFrame(filelist).transpose()#.reset_index().rename(columns={'index':'fileid'})
    dbitems=db.copy() #db.loc[db.filename.str.contains('TNS')].copy()
    subs=[]
    for fname in dbitems.filename:
        try:
            #print(f)
            subjvscan = fname[fname.find('HCA'):fname.find('HCA')+15]
            l2=subjvscan.split('_')
            row=l2+[fname]
            print(row)
            rowfor=pd.DataFrame(row).transpose()
            print(rowfor)
            anydata=pd.concat([anydata,rowfor])
        except:
            print("problem with BOX file",f)
anydata.to_csv(outp+"temp_psychopy.csv",index=False)

#part two (scan intradb)
psychointradb4 = run_ssh_cmd('plenzini@login3.chpc.wustl.edu',
                       "ls /ceph/intradb/archive/AABC_WU_ITK/arc001/*/RESOURCES/LINKED_DATA/PSYCHOPY/ | cut -d'_' -f2,3,4 | grep HCA | grep -E -v 'ITK|Eye|tt' | sort -u").stdout.read()
psychointradb3 = run_ssh_cmd('plenzini@login3.chpc.wustl.edu',
                       "ls /ceph/intradb/archive/AABC_UMN_ITK/arc001/*/RESOURCES/LINKED_DATA/PSYCHOPY/ | cut -d'_' -f2,3,4 | grep HCA | grep -E -v 'ITK|Eye|tt' | sort -u").stdout.read()
psychointradb2 = run_ssh_cmd('plenzini@login3.chpc.wustl.edu',
                       "ls /ceph/intradb/archive/AABC_UCLA_ITK/arc001/*/RESOURCES/LINKED_DATA/PSYCHOPY/ | cut -d'_' -f2,3,4 | grep HCA | grep -E -v 'ITK|Eye|tt' | sort -u").stdout.read()
psychointradb1 = run_ssh_cmd('plenzini@login3.chpc.wustl.edu',
                       "ls /ceph/intradb/archive/AABC_MGH_ITK/arc001/*/RESOURCES/LINKED_DATA/PSYCHOPY/ | cut -d'_' -f2,3,4 | grep HCA | grep -E -v 'ITK|Eye|tt' | sort -u").stdout.read()
df4 = pd.DataFrame(str.splitlines(psychointradb4.decode('utf-8')))
df4 = df4[0].str.split(',', expand=True)
df3 = pd.DataFrame(str.splitlines(psychointradb3.decode('utf-8')))
df3 = df3[0].str.split(',', expand=True)
df2 = pd.DataFrame(str.splitlines(psychointradb2.decode('utf-8')))
df2 = df2[0].str.split(',', expand=True)
df1 = pd.DataFrame(str.splitlines(psychointradb1.decode('utf-8')))
df1 = df1[0].str.split(',', expand=True)

df=pd.concat([df1,df2,df3,df4],axis=0) #df2,

df.to_csv(outp+"temp_psychintradb.csv",index=False)
