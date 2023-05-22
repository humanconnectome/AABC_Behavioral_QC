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


client = box.get_client()
Hotfolder = config['NonQBox']['Hotflash']['Allsites']
HotSites = folder_files(client,[Hotfolder])
HotFiles=pd.DataFrame()
for i in HotSites:  # [0:3]:
    #subfilelist = box.list_of_files([i])
    f = client.folder(folder_id=i).get()
    subfilelist=list(f.get_items())
    if subfilelist != []:
        subdb = pd.DataFrame([str(a) for a in subfilelist])#.transpose()
        #subdb['PIN'] = str(f)[str(f).find('HC'):str(f).find('HC') + 13].strip(' ')
        new = subdb[0].str.split('(', expand=True)
        subdb['PIN']=new[1].str[:13]#.str.split(' ',expand=True)[[0]]
        HotFiles = HotFiles.append(subdb)

HotFiles.to_csv(outp+"temp_Hotties.csv",index=False)
