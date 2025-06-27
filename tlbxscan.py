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

# NOW FOR TOOLBOX. ############################################################################
# # 1. grab partial files from intraDB
# # 2. QC (after incorporating patches)
# # 3. generate tickets and send to JIra if don't already exist
# # 4. send tickets that arent identical to ones already in Jira
# # 5. concatenate legit data (A scores file and a Raw file, no test subjects or identical duplicates -- no 'Narrow' or 'Registration' datasets)
# # 6. create and send snapshot of patched data to BOX after dropping restricted variables

##FIRST THE RAW DATA FILES
tlbxdate="2025-06-17"

#navigate to plenzini@login3.chpc.wustl.edu:/home/tools/catTLBX and run
#./getTLBX_last.sh AABC_UMN_ITK "Assessment Data" DataUMN.csv#
#for all the ITK folders and string types

#tlbxraw4=importTLBX(siteabbrev='WU',typed='raw')
#tlbxraw1=importTLBX(siteabbrev='MGH',typed='raw')
#tlbxraw3=importTLBX(siteabbrev='UMN',typed='raw')
#tlbxraw2=importTLBX(siteabbrev='UCLA',typed='raw')
tlbxraw4=pd.read_csv(outp+"DataWU_"+tlbxdate+".csv",low_memory=False)
tlbxraw1=pd.read_csv(outp+"DataMGH_"+tlbxdate+".csv",low_memory=False)
tlbxraw3=pd.read_csv(outp+"DataUMN_"+tlbxdate+".csv",low_memory=False)
tlbxraw2=pd.read_csv(outp+"DataUCLA_"+tlbxdate+".csv",low_memory=False)

#send this to temp
rf2=pd.concat([tlbxraw1,tlbxraw2,tlbxraw3,tlbxraw4])
outp="/Users/petralenzini/work/Behavioral/AABC/AABC_Behavioral_QC/AABC_Behavioral_QC/tmp/"
rf2.to_csv(outp+"temp_TLBX_RAW.csv",index=False)


#NOW THE SCORED DATA
#tlbxscore4=importTLBX(siteabbrev='WU',typed='scores')
#tlbxscore1=importTLBX(siteabbrev='MGH',typed='scores')
#tlbxscore3=importTLBX(siteabbrev='UMN',typed='scores')
#tlbxscore2=importTLBX(siteabbrev='UCLA',typed='scores')
tlbxscore4=pd.read_csv(outp+"ScoresWU_"+tlbxdate+".csv",low_memory=False)
tlbxscore1=pd.read_csv(outp+"ScoresMGH_"+tlbxdate+".csv",low_memory=False)
tlbxscore3=pd.read_csv(outp+"ScoresUMN_"+tlbxdate+".csv",low_memory=False)
tlbxscore2=pd.read_csv(outp+"ScoresUCLA_"+tlbxdate+".csv",low_memory=False)



dffull=pd.concat([tlbxscore1, tlbxscore3, tlbxscore2, tlbxscore4])
dffull.to_csv(outp+"temp_TLBX_Scores.csv",index=False)

#NOW THE Registration DATA
#tlbxscore4=importTLBX(siteabbrev='WU',typed='scores')
#tlbxscore1=importTLBX(siteabbrev='MGH',typed='scores')
#tlbxscore3=importTLBX(siteabbrev='UMN',typed='scores')
#tlbxscore2=importTLBX(siteabbrev='UCLA',typed='scores')
tlbxReg4=pd.read_csv(outp+"RegistWU_"+tlbxdate+".csv",low_memory=False)
tlbxReg1=pd.read_csv(outp+"RegistMGH_"+tlbxdate+".csv",low_memory=False)
tlbxReg3=pd.read_csv(outp+"RegistUMN_"+tlbxdate+".csv",low_memory=False)
tlbxReg2=pd.read_csv(outp+"RegistUCLA_"+tlbxdate+".csv",low_memory=False)

tlbxReg1['site']=1
tlbxReg2['site']=2
tlbxReg3['site']=3
tlbxReg4['site']=4

dfreg=pd.concat([tlbxReg1, tlbxReg3, tlbxReg2, tlbxReg4])
dfreg.to_csv(outp+"temp_TLBX_Registration.csv",index=False)
