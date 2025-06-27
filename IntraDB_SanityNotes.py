import pandas as pd
from functions import *
from config import *
import os
from datetime import date
import subprocess
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
projects=["AABC_MGH_ITK","AABC_UMN_ITK","AABC_UCLA_ITK","AABC_WU_ITK"]
freeze2list=pd.read_csv('/Users/petralenzini/work/Behavioral/AABC/AABC_Behavioral_QC/AABC_Behavioral_QC/Union_HCA-AABC_Data-Freeze-2_id-events_2025-04-25.csv')
#test
#curlcmd0 = "curl -s -k -v -u "+user+":"+passw+" https://intradb.humanconnectome.org/data/experiments?xsiType=xnat:mrSessionData\&format=json\&columns=ID,label,project,xsiType,subject_label,URI\&project=AABC_MGH_ITK"
#curlcmd0 = "curl -s -k -v -u "+user+":"+passw+" https://intradb.humanconnectome.org/data/experiments?xsiType=xnat:subjectData\&format=json\&columns=subject_label,yob\&project=AABC_MGH_ITK"

#PROJECT='AABC_MGH_ITK'
AllSanity=pd.DataFrame()
for PROJECT in projects:
    curlcmd="curl -s -k -v -u "+user+":"+passw+" https://"+HOST+"/xapi/sanityChecksReports/project/"+PROJECT+"/failureReportCSV > "+outp+PROJECT+"_Sanity.csv"
    os.system(curlcmd)
    sanitydf=pd.read_csv(outp+PROJECT+"_Sanity.csv")
    sanitydf=sanitydf.loc[~(sanitydf.scanID.isin(["10-MR1","10-MR2"]))]
    sanitydf['scanID'] = sanitydf['scanID'].fillna(-1)
    sanitydf=sanitydf.loc[sanitydf.scanner.isnull()==False].copy()
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

    SanityChecksWithNotes=pd.merge(sanitydf,scannotes,how='left',on=['imageSessionLabel','scanID'])
    SanityChecksWithNotes['PROJECT']=PROJECT
    AllSanity=pd.concat([AllSanity,SanityChecksWithNotes],axis=0)
    SanityChecksWithNotes.to_csv(outp+'SanityChecksWithNotes'+PROJECT+"_"+date.today().strftime("%d%b%Y")+'.csv',index=False)

AllSanity['imageSessionDate']=pd.to_datetime(AllSanity.imageSessionDate)
#AllSanity['Freeze1']=AllSanity.loc[AllSanity.imageSessionDate<"11/01/2023"]

### These commented out filters were to drop sanity check failures due to XA30 when generating lists to sites
### -- this issue should be resolved post-freeze, so we won't filter them out again without good reason
### Wash U: XA30 scanner = AWP166158
### UCLA: XA30 scanner = MRC35426
### MGH: all scans after 10/11/23
print(AllSanity.shape)
#sub1=AllSanity.loc[~((AllSanity.PROJECT=="AABC_MGH_ITK") & (AllSanity.imageSessionDate>"2023-10-11"))]
#print(sub1.shape)
#sub2=sub1.loc[~((sub1.scanner=="MRC35426") & (sub1.imageSessionDate>"4/20/2023"))]
#print(sub2.shape)
#sub3=sub2.loc[~((sub2.scanner=="MRC35343") & (sub2.imageSessionDate>"10/26/2023"))]
#print(sub3.shape)
#sub4=sub3.loc[~(sub3.scanner.isin(['AWP166158']))].copy()
#print(sub4.shape)
#sub5=sub4.loc[sub4.imageSessionDate<"2023-11-01"]
#print(sub5.shape)
#sub5=sub5.loc[~((sub5.imageSessionLabel.str.contains('TEST')) | (sub5.imageSessionLabel.str.contains('incomplete')) | (sub5.imageSessionLabel.str.contains('7T')) | (sub5.imageSessionLabel.str.contains('phantom')))]

#drop stuff we know is bogus:
AllSanity=AllSanity.loc[~(AllSanity.imageSessionLabel.str.upper().str.contains("TEST"))]
print(AllSanity.shape)
AllSanity=AllSanity.loc[~(AllSanity.imageSessionLabel.str.upper().str.contains("PHANTOM"))]
AllSanity=AllSanity.loc[~(AllSanity.imageSessionLabel.str.upper().str.contains("AGAR"))]

AllSanity[['imageSessionLabel','PROJECT']]
worstsubs=AllSanity.imageSessionLabel.value_counts().reset_index()
worstsubs.columns=['imageSessionLabel','misdemeanors']

pd.merge(worstsubs,AllSanity[['imageSessionLabel','PROJECT','imageSessionDate']].drop_duplicates(),on='imageSessionLabel',how='left').to_csv(outp+'WorstSessions_'+date.today().strftime("%d%b%Y")+'.csv')
AllSanity.PROJECT.value_counts()
AllSanity=AllSanity.drop(columns=['imageSessionID','label','site','ID'])
AllSanity=AllSanity.rename(columns={'':'MR_Session'})
AllSanity.to_csv(outp+'SanityChecksWithNotes_'+date.today().strftime("%d%b%Y")+'.csv',index=False)


inventory=pd.read_csv('Union-Freeze_AABC-HCA_VIV_2024-05-13.csv')
inventory=inventory[['Site','PIN']].copy()
inventory.to_csv('FreezeTemp.csv',index=False)
allscans=pd.read_csv('HCAABC_ITK.csv',low_memory=False)

allscans['PIN']=allscans['MR ID'].str[:13]
allscans.to_csv('ITK.csv',index=False)
check=allscans.merge(inventory,on='PIN',how='right').copy()
m7=check[(check['MR ID'].str.contains('7T')==True) | (check['MR ID'].str.upper().str.contains('MRS')==True)][['MR ID','PIN','Date']].drop_duplicates()
d3A=check[check['MR ID'].str.contains('_A')==True][['MR ID','PIN','Date']].drop_duplicates()
d3B=check[check['MR ID'].str.contains('_B')==True][['MR ID','PIN','Date']].drop_duplicates()
MRS=check.loc[check.PIN.isin(list7)]#.to_csv('MRSdates.csv',index=False)
A=check.loc[check.PIN.isin(list3A)]
B=check.loc[check.PIN.isin(list3B)]
MRSmerge=m7.rename(columns={'Date':'7T date','MR ID':'7T MR ID'}).merge(d3A.rename(columns={'Date':'3T Session A Date','MR ID':'3T Session A MR ID'}),how='left',on='PIN')
MRSmerge=MRSmerge.merge(d3B.rename(columns={'Date':'3T Session B Date','MR ID':'3T Session B MR ID'}),how='left',on='PIN')
#MRSmerge['3A to 7T days passed']=(date(MRSmerge['7T date'])-date(MRSmerge['3T Session A Date']))
MRSmerge.drop(columns=['PIN'])[['3T Session A MR ID','3T Session A Date','3T Session B MR ID','3T Session B Date','7T MR ID','7T date']].to_csv('MRSdates.csv',index=False)


#compare status of INTAKE to STG directory for Freeze2
def getsessionlist(proj = "AABC_STG"):
    # Build the curl command
    curlcmd = [
        "curl", "-s", "-k", "-u", f"{user}:{passw}",
        f"https://intradb.humanconnectome.org/data/experiments?xsiType=xnat:mrSessionData&format=json&columns=ID,label,project,xsiType,subject_label,URI&project={proj}"
    ]
    # Run curl and capture the output
    try:
        result = subprocess.check_output(curlcmd, text=True)
        data = json.loads(result)
        # Extract label values
        labels = [item['label'] for item in data['ResultSet']['Result']]
        return labels

    except subprocess.CalledProcessError as e:
        print("Curl command failed:", e)
    except json.JSONDecodeError as e:
        print("Failed to parse JSON:", e)
    except KeyError as e:
        print("Unexpected data format, missing key:", e)

#get STG list
S=getsessionlist(proj = "AABC_STG")
H=getsessionlist(proj = "CCF_HCA_STG")

STG=pd.DataFrame(S+H)
STG[['subject', 'redcap_event','other']] = STG[0].str.split('_', n=3, expand=True)
STG['STG']='YES'

#Get itk lists
MGH=getsessionlist(proj = "AABC_MGH_ITK")
UMN=getsessionlist(proj = "AABC_UMN_ITK")
WU=getsessionlist(proj = "AABC_WU_ITK")
UCLA=getsessionlist(proj = "AABC_UCLA_ITK")

ITK=pd.DataFrame(MGH+UMN+WU+UCLA)
ITK[['subject', 'redcap_event','other']] = ITK[0].str.split('_', n=3, expand=True)
ITK['AABC_ITK']='YES'
#merge STG and ITK lists with freeze2 list
FZa = pd.merge(freeze2list[['study', 'subject', 'redcap_event', 'M/F', 'Site', 'event_date', 'Freeze']],ITK[['subject','redcap_event','AABC_ITK']].drop_duplicates(),on=['redcap_event','subject'],how='left')
FZb = pd.merge(FZa,STG[['subject','redcap_event','STG']].drop_duplicates(),on=['redcap_event','subject'],how='left')
FZb.to_csv('Union_HCA-AABC_Data-Freeze-2_ImgStatus_2025-04-25.csv',index=False)
