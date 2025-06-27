import pandas as pd
import yaml
import ccf
from ccf.box import LifespanBox
#import requests
import re
import collections
#from functions import *
#import functions
from config import *
#import subprocess
import os
import sys
from datetime import date

## get configuration files
print('getting creds')

outp="/Users/petralenzini/work/Behavioral/AABC/AABC_Behavioral_QC/AABC_Behavioral_QC/tmp/"
config = LoadSettings("/Users/petralenzini/work/Behavioral/AABC/AABC_Behavioral_QC/AABC_Behavioral_QC/config.yml")
secret=pd.read_csv(config['config_files']['secrets'])
print('connecting to box')

box = LifespanBox(cache=outp)
scratch=205351313707

#scan BOX
folderqueue=['UMN','WU','MGH']#,'UCLA']
actdata=[]
#studyshort='WU'
for studyshort in folderqueue:
    print(studyshort)
    folder = config['NonQBox']['Actigraphy'][studyshort]
    dag = config['Redcap']['datasources']['aabcarms'][studyshort]['dag']
    sitenum = config['Redcap']['datasources']['aabcarms'][studyshort]['sitenum']
    filelist=box.list_of_files([folder])
    db=pd.DataFrame(filelist).transpose()#.reset_index().rename(columns={'index':'fileid'})
    dbitems=db.copy() #db.loc[db.filename.str.contains('TNS')].copy()
    actsubs=[]
    for fid in dbitems.fileid:
        try:
            print('looking for id')
            patrn = 'Identity'
            f=box.downloadFile(fid, download_dir="tmp", override_if_exists=False)
            print(f)
            file_one = open(f, "r")
            print(dir(file_one))
            variable = file_one.readline(1)
            if not variable=='':
                for l in file_one.readlines():
                    if re.search(patrn, l):
                        hcaid=''
                        hcaid=l.strip("\n").replace('"', '').split(',')[1]
                        print("Inner",f,"has",hcaid)
                        actsubs=actsubs+[hcaid]
            file_one.close()
            os.remove(outp+f.split('/')[1])
        except:
            print("Something the matter with file",f)
    actdata=actdata+list(actsubs)#list(set(actsubs))


pd.DataFrame(actdata,columns=['PIN']).to_csv(outp+"temp_actigraphy.csv",index=False)
