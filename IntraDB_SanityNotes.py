import pandas as pd
from functions import *
from config import *
import os
from datetime import date
import json
import datetime

## get configuration files
outp="/Users/petralenzini/work/Behavioral/AABC/AABC_Behavioral_QC/AABC_Behavioral_QC/tmp/"
config = LoadSettings("/Users/petralenzini/work/Behavioral/AABC/AABC_Behavioral_QC/AABC_Behavioral_QC/config.yml")
secret=pd.read_csv(config['config_files']['secrets'])
intradb=pd.read_csv(config['config_files']['PCP'])
user = intradb.user[0]
passw = intradb.auth[0]
HOST = "hcpi-shadow22.nrg.wustl.edu"
projects=["AABC_UMN_ITK","AABC_UCLA_ITK","AABC_MGH_ITK"]#"AABC_WU_ITK",
#test
#curlcmd0 = "curl -s -k -v -u "+user+":"+passw+" https://intradb.humanconnectome.org/data/experiments?xsiType=xnat:mrSessionData\&format=json\&columns=ID,label,project,xsiType,subject_label,URI\&project=AABC_MGH_ITK"
#curlcmd0 = "curl -s -k -v -u "+user+":"+passw+" https://intradb.humanconnectome.org/data/experiments?xsiType=xnat:subjectData\&format=json\&columns=subject_label,yob\&project=AABC_MGH_ITK"

##os.system(curlcmd0)
#os.system(curlcmd0)
PROJECT="AABC_WU_ITK"
AllSanity=pd.DataFrame()
for PROJECT in projects:
    curlcmd="curl -s -k -v -u "+user+":"+passw+" https://"+HOST+"/xapi/sanityChecksReports/project/"+PROJECT+"/failureReportCSV > "+outp+PROJECT+"_Sanity.csv"
    os.system(curlcmd)
    sanitydf=pd.read_csv(outp+PROJECT+"_Sanity.csv")
    sanitydf['scanID'] = sanitydf['scanID'].fillna(-1)
    sanitydf['scanID'] = sanitydf['scanID'].astype(int)
    sanitydf['scanID'] = sanitydf['scanID'].astype(str)
    sanitydf['scanID'] = sanitydf['scanID'].replace('-1', '')

    curlcmd1="curl -s -k -v -u "+user+":"+passw+" 'https://intradb.humanconnectome.org/xapi/scanNotesReports/project/"+PROJECT+"/dataTable' > "+outp+PROJECT+"_ScanNotes.txt"
    os.system(curlcmd1)
    with open(outp + PROJECT + "_ScanNotes.txt", 'r') as file:
        data = json.load(file)
    scannotes = pd.DataFrame(data)[['expLabel','scanId','scanQuality','scanNote','sessionNote']]
    scannotes.columns=['imageSessionLabel','scanID','scanQuality','scanNote','sessionNote']
    scannotes['scanID']=scannotes.scanID.astype(int).astype(str)
    scannotes.to_csv(outp+PROJECT+"_ScanNotes.csv")
    #curlcmd2="curl -s -k -v -u "+user+":"+passw+" https://"+HOST+"/data/experiments?xsiType=xnat:mrSessionData\&format=json\&columns=ID,label,project,xsiType,scanner,subject_label,session_type,URI,note\&project="+PROJECT+" > "+outp+PROJECT+"_Notes.txt"
    #os.system(curlcmd2)
    #with open(outp+PROJECT+"_Notes.txt", 'r') as file:
    #    data = json.load(file)
    #notesdf = pd.json_normalize(data['ResultSet']['Result'])
    #notesdf['sessionLabel']=notesdf.label
    #notesdf=notesdf[['note','sessionLabel']].copy()
    #notesdf['label']=notesdf.sessionLabel+"_sc"

    SanityChecksWithNotes=pd.merge(sanitydf,scannotes,how='left',on=['imageSessionLabel','scanID'])
    SanityChecksWithNotes['PROJECT']=PROJECT
    AllSanity=pd.concat([AllSanity,SanityChecksWithNotes],axis=0)
    SanityChecksWithNotes.to_csv(outp+'SanityChecksWithNotes'+PROJECT+"_"+date.today().strftime("%d%b%Y")+'.csv',index=False)

AllSanity['imageSessionDate']=pd.to_datetime(AllSanity.imageSessionDate)

#Wash U: XA30 scanner = AWP166158
#UCLA: XA30 scanner = MRC35426
#MGH: all scans after 10/11/23
print(AllSanity.shape)
sub1=AllSanity.loc[~((AllSanity.PROJECT=="AABC_MGH_ITK") & (AllSanity.imageSessionDate>"2023-10-11"))]
print(sub1.shape)
sub2=sub1.loc[~((sub1.scanner=="MRC35426") & (sub1.imageSessionDate>"4/20/2023"))]
print(sub2.shape)
sub3=sub2.loc[~((sub2.scanner=="MRC35343") & (sub2.imageSessionDate>"10/26/2023"))]
print(sub3.shape)
sub4=sub3.loc[~(sub3.scanner.isin(['AWP166158']))].copy()
print(sub4.shape)
sub5=sub4.loc[sub4.imageSessionDate<"2023-11-01"]
print(sub5.shape)
sub5=sub5.loc[~((sub5.imageSessionLabel.str.contains('TEST')) | (sub5.imageSessionLabel.str.contains('incomplete')) | (sub5.imageSessionLabel.str.contains('7T')) | (sub5.imageSessionLabel.str.contains('phantom')))]


sub5.imageSessionLabel.value_counts()
sub5.PROJECT.value_counts()
sub5=sub5.drop(columns=['imageSessionID','label','site','ID'])
sub5=sub5.rename(columns={'imageSessionLabel':'MR_Session'})
sub5.to_csv(outp+'SanityChecksWithNotes_'+date.today().strftime("%d%b%Y")+'.csv',index=False)