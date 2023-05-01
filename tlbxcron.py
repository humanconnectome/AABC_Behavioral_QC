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

# NOW FOR TOOLBOX. ############################################################################
# # 1. grab partial files from intraDB
# # 2. QC (after incorporating patches)
# # 3. generate tickets and send to JIra if don't already exist
# # 4. send tickets that arent identical to ones already in Jira
# # 5. concatenate legit data (A scores file and a Raw file, no test subjects or identical duplicates -- no 'Narrow' or 'Registration' datasets)
# # 6. create and send snapshot of patched data to BOX after dropping restricted variables

##FIRST THE RAW DATA FILES
tlbxraw4=importTLBX(siteabbrev='WU',typed='raw')
tlbxraw1=importTLBX(siteabbrev='MGH',typed='raw')
tlbxraw3=importTLBX(siteabbrev='UMN',typed='raw')
tlbxraw2=importTLBX(siteabbrev='UCLA',typed='raw')

#send this to temp
rf2=pd.concat([tlbxraw1,tlbxraw2,tlbxraw3,tlbxraw4])
outp="/Users/petralenzini/work/Behavioral/AABC/AABC_Behavioral_QC/AABC_Behavioral_QC/tmp/"
rf2.to_csv(outp+"temp_TLBX_RAW.csv",index=False)


#NOW THE SCORED DATA
tlbxscore4=importTLBX(siteabbrev='WU',typed='scores')
tlbxscore1=importTLBX(siteabbrev='MGH',typed='scores')
tlbxscore3=importTLBX(siteabbrev='UMN',typed='scores')
tlbxscore2=importTLBX(siteabbrev='UCLA',typed='scores')
tlbxscore1['site']=1
tlbxscore2['site']=2
tlbxscore3['site']=3
tlbxscore4['site']=4


dffull=pd.concat([tlbxscore1, tlbxscore3, tlbxscore2, tlbxscore4])
dffull.to_csv(outp+"temp_TLBX_Scores.csv",index=False)