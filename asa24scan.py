import pandas as pd
import pandas as pd
import yaml
import ccf
from ccf.box import LifespanBox
import requests
import re
import collections
from functions import *
from config import *
import subprocess
import os
import sys
from datetime import date

## get configuration files
outp="/Users/petralenzini/work/Behavioral/AABC/AABC_Behavioral_QC/AABC_Behavioral_QC/tmp/"
config = LoadSettings("/Users/petralenzini/work/Behavioral/AABC/AABC_Behavioral_QC/AABC_Behavioral_QC/config.yml")
secret=pd.read_csv(config['config_files']['secrets'])
box = LifespanBox(cache=outp)
#scratch=205351313707

folderqueue=['MGH','WU','UMN','UCLA']
client = box.get_client()
ALLSUBS,BIGGESTTotals,BIGGESTItems,BIGGESTResp,BIGGESTTS,BIGGESTTNS,BIGGESTINS=getASA(client=client,folderqueue=folderqueue)
ALLSUBS.to_csv(outp+'temp_ALLSUBS.csv',index=False)
BIGGESTTotals.to_csv(outp+'temp_Totals.csv',index=False)
BIGGESTItems.to_csv(outp+'temp_Items.csv',index=False)
BIGGESTResp.to_csv(outp+'temp_Resp.csv',index=False)
BIGGESTTS.to_csv(outp+'temp_TTS.csv',index=False)
BIGGESTTNS.to_csv(outp+'temp_TNS.csv',index=False)
BIGGESTINS.to_csv(outp+'temp_INS.csv',index=False)

print("THE END")
