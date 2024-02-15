import pandas as pd
from functions import *
from config import *
import os
from datetime import date
import json

## get configuration files
outp="/Users/petralenzini/work/Behavioral/AABC/AABC_Behavioral_QC/AABC_Behavioral_QC/tmp/"
config = LoadSettings("/Users/petralenzini/work/Behavioral/AABC/AABC_Behavioral_QC/AABC_Behavioral_QC/config.yml")
secret=pd.read_csv(config['config_files']['secrets'])
intradb=pd.read_csv(config['config_files']['PCP'])
user = intradb.user[0]
passw = intradb.auth[0]
HOST = "hcpi-shadow22.nrg.wustl.edu"
projects=["AABC_WU_ITK","AABC_UMN_ITK","AABC_UCLA_ITK","AABC_MGH_ITK"]

for PROJECT in projects:
    curlcmd="curl -s -k -v -u "+user+":"+passw+" https://"+HOST+"/xapi/sanityChecksReports/project/"+PROJECT+"/failureReportCSV > "+outp+PROJECT+"_Sanity.csv"
    os.system(curlcmd)
    sanitydf=pd.read_csv(outp+PROJECT+"_Sanity.csv")

    curlcmd2="curl -s -k -v -u "+user+":"+passw+" https://"+HOST+"/data/experiments?xsiType=xnat:mrSessionData\&format=json\&columns=ID,label,project,xsiType,scanner,subject_label,session_type,URI,note\&project="+PROJECT+" > "+outp+PROJECT+"_Notes.txt"
    os.system(curlcmd2)
    with open(outp+PROJECT+"_Notes.txt", 'r') as file:
        data = json.load(file)
    notesdf = pd.json_normalize(data['ResultSet']['Result'])
    notesdf['sessionLabel']=notesdf.label
    notesdf=notesdf[['note','sessionLabel']].copy()
    notesdf['label']=notesdf.sessionLabel+"_sc"

    SanityChecksWithNotes=pd.merge(sanitydf,notesdf,how='left',on='label')
    SanityChecksWithNotes.to_csv(outp+'SanityChecksWithNotes'+PROJECT+"_"+date.today().strftime("%d%b%Y")+'.csv',index=False)

