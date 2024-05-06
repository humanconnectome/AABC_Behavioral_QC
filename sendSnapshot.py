import pandas as pd
from ccf.box import LifespanBox
import re
import collections
from functions import *
from config import *
from datetime import date
import requests

config = LoadSettings()
secret=pd.read_csv(config['config_files']['secrets'])
intradb=pd.read_csv(config['config_files']['PCP'])
box = LifespanBox(cache="./tmp")
outp="/Users/petralenzini/work/Behavioral/AABC/AABC_Behavioral_QC/AABC_Behavioral_QC/tmp/"


###############
#Various specialty lists of subjects
DNR = ["HCA7142156_V1","HCA7787304_V1", "HCA6276071_V1", "HCA6229365_V1", "HCA9191078_V1", "HCA6863086_V1"]
DNRsubs = ["HCA7142156","HCA7787304", "HCA6276071", "HCA6229365", "HCA9191078", "HCA6863086"]
#These guys accidentally recruited as V2 imaging for AABC - were behavior only in HCA, so drop from HCA list
v2oops=['HCA6686191_V2','HCA7296183_V2']

freeze=pd.read_csv(outp+"Freeze1_n439_12Feb2024_MR_ID.csv")
freeze.columns=['PIN']
freezelist=list(freeze.PIN)
freezesubs=pd.read_csv(outp+"Freeze1_n439_12Feb2024.csv")
freezesubs.columns=['subject']
freezesubjects=list(freezesubs['subject'])

pathp=box.downloadFile(config['hcainventory'])
ids=pd.read_csv(pathp)[['PIN','subject','redcap_event','IntraDB']]
HCAlist=list(ids.loc[(~(ids.IntraDB=='CCF_PCMP_ITK')) & (ids.redcap_event.isin(['V1','V2'])) & (~(ids.PIN.isin(v2oops)))]['PIN'])
HCAsubjects=list(ids.loc[(~(ids.IntraDB=='CCF_PCMP_ITK')) & (ids.redcap_event.isin(['V1','V2'])) & (~(ids.PIN.isin(v2oops)))]['subject'].unique())

#Encyclopedia
E=pd.read_csv(box.downloadFile(config['encyclopedia']),low_memory=False,encoding='ISO-8859-1')
#SSAGAvars=list(E.loc[E['Form / Instrument'].str.upper().str.contains('SSAGA'),'Variable / Field Name'])

#some folders for box downloads/uploads
Asnaps=config['aabc_pre']
Rsnaps=config['aabc_pre_restricted']
freezefolder=config['unionfreeze1']
mrsfile=config['mrsRatoi']

#DO a Fresh download of everything, drop all issues, and send snapshot.
#Use inventory as gold standard and make sure visit is complete.
aabcreport = redreport(tok=secret.loc[secret.source=='aabcarms','api_key'].reset_index().drop(columns='index').api_key[0],reportid='51031')
keepvars=['study_id','subject_id','redcap_event_name','site','v0_date','event_date','age','age_visit','sex','ethnic','racial','legacy_yn','height_ft','height_in','weight','bmi','psuedo_guid','scan_collectyn']
aabcinvent=getframe(struct=aabcreport,api_url=config['Redcap']['api_url10'])
completevars=[i for i in aabcinvent.columns if 'complete' in i and '___' not in i]
inventoryaabc=idvisits(aabcinvent,keepsies=keepvars+completevars)
print(inventoryaabc.shape)
print(inventoryaabc.redcap_event.value_counts())

#remove anyoneo in DNR list
# don't remove v2oops - only for the union - drop behavioral data from HCA for the v2oops
inventoryaabc['PIN']=inventoryaabc.subject+"_"+inventoryaabc.redcap_event
print(inventoryaabc.shape)
inventoryaabc=inventoryaabc.loc[~(inventoryaabc.subject.isin(DNRsubs))]

#check:
inventoryaabc.loc[inventoryaabc.PIN.isin(list(freeze.PIN))]
len(inventoryaabc.loc[inventoryaabc.subject.isin(freezesubjects)]['subject'].unique())

inventoryaabc['event_age']=inventoryaabc.age
inventoryaabc.loc[inventoryaabc.event_age=='','event_age']=inventoryaabc.age_visit
print(inventoryaabc.shape)
print(inventoryaabc.redcap_event.value_counts())

#remove test subjects if they exist
subjects=inventoryaabc[['study_id','register_visit_complete','PIN']]
inventoryaabc = inventoryaabc.loc[~(inventoryaabc.subject_id.str.upper().str.contains('TEST'))].copy()
print(inventoryaabc.shape)
print(inventoryaabc.redcap_event.value_counts())

#subset to people who have come in for an inperson visit
# TO DO: account for second visit
subjects=subjects.loc[subjects.register_visit_complete =='2'][['study_id']]
subs=list(subjects.study_id.unique())

#harmonize event date
inventoryaabc=inventoryaabc.loc[inventoryaabc.study_id.isin(subs)].copy()
inventoryaabc.loc[inventoryaabc.event_date=='','event_date']=inventoryaabc.v0_date
inventoryaabc=inventoryaabc.drop(columns=['v0_date'])
inventoryaabc=inventoryaabc.loc[~(inventoryaabc.event_date=='')]
print(inventoryaabc.shape)
print(inventoryaabc.redcap_event.value_counts())

#drop those with missing ids
#grab PINs to drop datatypes
# remap redcap_event BEFORE FU
inventorysnapshot=inventoryaabc.copy()
inventorysnapshot['race']=inventorysnapshot.replace({'racial':
                                       {'1':'American Indian/Alaska Native',
                                        '2':'Asian',
                                        '3':'Black or African American',
                                        '4':'Hawaiian or Pacific Islander',
                                        '5':'White',
                                        '6':'More than one race',
                                        '99':'Unknown or not reported'}})['racial']
inventorysnapshot['ethnic_group']=inventorysnapshot.replace({'ethnic':
                                           {'1':'Hispanic or Latino',
                                            '2':'Not Hispanic or Latino',
                                            '3':'unknown or not reported'}})['ethnic']
inventorysnapshot['M/F']=inventorysnapshot.replace({'sex':
                                           {'1':'M',
                                            '2':'F'}})['sex']
inventorysnapshot['Site']=inventorysnapshot.site.replace({'1':'MGH','2':'UCLA','3':'UMinn','4':'WashU'})
inventorysnapshot=inventorysnapshot.drop(columns={'sex','racial','ethnic','site','subject_id','age','age_visit'})

inventorysnapshot=rollforward(inventorysnapshot,'legacy_yn','AF0')
inventorysnapshot=rollforward(inventorysnapshot,'race','AF0')
inventorysnapshot=rollforward(inventorysnapshot,'ethnic_group','AF0')
inventorysnapshot=rollforward(inventorysnapshot,'M/F','AF0')
inventorysnapshot=rollforward(inventorysnapshot,'Site','AF0')

inventorysnapshot=inventorysnapshot.drop_duplicates(subset=['subject','redcap_event']).copy()


#get lists of issues by type
#issuesfile='All_Issues_'+date.today().strftime("%d%b%Y")+'.csv'
issuesfile='./All_Issues_'+date.today().strftime("%d%b%Y")+'.csv'
issues=pd.read_csv(issuesfile)
issues=issues.loc[(issues.subject.isnull()==False)& (issues.datatype !="REDCap")].copy()
issues['PIN']=issues.subject+"_"+issues.redcap_event
issues=issues[['PIN','datatype','subject','redcap_event']].copy()
len(issues.PIN.unique())

Actissues=list(issues.loc[issues.datatype=='Actigraphy']['PIN'])
TLBXissues=list(issues.loc[issues.datatype=='TLBX']['PIN'])
RAVLTissues=list(issues.loc[issues.datatype=='RAVLT']['PIN'])
HotFlashissues=list(issues.loc[issues.datatype=='HotFlash']['PIN'])
ASAissues=list(issues.loc[issues.datatype=='ASA24']['PIN'])


################ CREATE SNAPSHOTS and then create a completeness inventory ############################
#ASA24
BIGGESTTotals=pd.read_csv(outp+'temp_Totals.csv')
BIGGESTItems=pd.read_csv(outp+'temp_Items.csv')
BIGGESTResp=pd.read_csv(outp+'temp_Resp.csv')
BIGGESTTS=pd.read_csv(outp+'temp_TTS.csv')
BIGGESTTNS=pd.read_csv(outp+'temp_TNS.csv')
BIGGESTINS=pd.read_csv(outp+'temp_INS.csv')

#subjects with issues get sorted out in the PINfirst function
BIGGESTTotalsRest,BIGGESTTotals2=PINfirst(BIGGESTTotals,strname="Totals",issuefile=issuesfile,inventory=inventorysnapshot[['subject','redcap_event']],dropvars=[]) #restrictedATotals);
BIGGESTItemsRest,BIGGESTItems2=PINfirst(BIGGESTItems,strname="Items",issuefile=issuesfile,inventory=inventorysnapshot[['subject','redcap_event']],dropvars=[]) #,restrictedAItems)
BIGGESTRespRest,BIGGESTResp2=PINfirst(BIGGESTResp,strname="Resp",issuefile=issuesfile,inventory=inventorysnapshot[['subject','redcap_event']],dropvars=[]) #,restrictedAResp)
BIGGESTTSRest,BIGGESTTS2=PINfirst(BIGGESTTS,strname="TS",issuefile=issuesfile,inventory=inventorysnapshot[['subject','redcap_event']],dropvars=[]) #,restrictedATS)
BIGGESTTNSRest,BIGGESTTNS2=PINfirst(BIGGESTTNS,strname="TNS",issuefile=issuesfile,inventory=inventorysnapshot[['subject','redcap_event']],dropvars=[]) #,restrictedATNS)
BIGGESTINSRest,BIGGESTINS2=PINfirst(BIGGESTINS,strname="INS",issuefile=issuesfile,inventory=inventorysnapshot[['subject','redcap_event']],dropvars=[]) #,restrictedAINS)
#check
BIGGESTTotalsRest.loc[BIGGESTTotalsRest.PIN.isin(DNR)]
BIGGESTTotalsRest.loc[BIGGESTTotalsRest.PIN.isin(ASAissues)]

#note there will be duplicates - duplicates are removed in slice but not freeze altogether.
BIGGESTTotals2Freeze=subsetfreeze(BIGGESTTotals2,"Totals",freezelist,byPIN=True)
BIGGESTItems2Freeze=subsetfreeze(BIGGESTItems2,"Items",freezelist,byPIN=True)
BIGGESTResp2Freeze=subsetfreeze(BIGGESTResp2,"Resp",freezelist,byPIN=True)
BIGGESTTS2Freeze=subsetfreeze(BIGGESTTS2,"TS",freezelist,byPIN=True)
BIGGESTTNS2Freeze=subsetfreeze(BIGGESTTNS2,"TNS",freezelist,byPIN=True)
BIGGESTINS2Freeze=subsetfreeze(BIGGESTINS2,"INS",freezelist,byPIN=True)

box.upload_file("./tmp/Freeze1_AABC_ASA24-Totals_" + date.today().strftime("%Y-%m-%d") + '.csv', freezefolder)
box.upload_file("./tmp/Freeze1_AABC_ASA24-Items_" + date.today().strftime("%Y-%m-%d") + '.csv', freezefolder)
box.upload_file("./tmp/Freeze1_AABC_ASA24-Resp_" + date.today().strftime("%Y-%m-%d") + '.csv', freezefolder)
box.upload_file("./tmp/Freeze1_AABC_ASA24-TS_" + date.today().strftime("%Y-%m-%d") + '.csv', freezefolder)
box.upload_file("./tmp/Freeze1_AABC_ASA24-TNS_" + date.today().strftime("%Y-%m-%d") + '.csv', freezefolder)
box.upload_file("./tmp/Freeze1_AABC_ASA24-INS_" + date.today().strftime("%Y-%m-%d") + '.csv', freezefolder)

box.upload_file("./tmp/AABC_"+"ASA24-"+ "Totals_" + date.today().strftime("%Y-%m-%d") + '.csv', Asnaps)
box.upload_file("./tmp/AABC_"+"ASA24-"+ "Items_" + date.today().strftime("%Y-%m-%d") + '.csv', Asnaps)
box.upload_file("./tmp/AABC_"+"ASA24-"+ "Resp_" + date.today().strftime("%Y-%m-%d") + '.csv', Asnaps)
box.upload_file("./tmp/AABC_"+"ASA24-"+ "TS_" + date.today().strftime("%Y-%m-%d") + '.csv', Asnaps)
box.upload_file("./tmp/AABC_"+"ASA24-"+ "TNS_" + date.today().strftime("%Y-%m-%d") + '.csv', Asnaps)
box.upload_file("./tmp/AABC_"+"ASA24-"+ "INS_" + date.today().strftime("%Y-%m-%d") + '.csv', Asnaps)
#  NOTHING RESTRICTED for ASA24 anymore

#qintdf2 will be already filtered for not unusables - inventory filtered for issues
qintreport = redreport(tok=secret.loc[secret.source=='qint','api_key'].reset_index().drop(columns='index').api_key[0],reportid='51037')
qintdf2=getframe(struct=qintreport,api_url=config['Redcap']['api_url10'])
qintdf2['redcap_event']="V"+qintdf2.visit
qintdf2=qintdf2.loc[~(qintdf2.q_unusable=='1')].copy()

qintdf2uploadrestricted=pd.merge(inventorysnapshot[['subject','redcap_event']],qintdf2,left_on=['subject','redcap_event'],right_on=['subjectid','redcap_event'],how='inner')
qintdf2uploadrestricted['PIN']=qintdf2uploadrestricted.subject+"_"+qintdf2uploadrestricted.redcap_event
qintdf2uploadrestricted=qintdf2uploadrestricted.loc[~(qintdf2uploadrestricted.PIN.isin(RAVLTissues))]
#reorganize columns
qintdf2uploadrestricted[['subject','redcap_event','PIN']+[i for i in qintdf2uploadrestricted.columns if i not in ['subject','redcap_event','PIN']]]
restrictedQ=list(E.loc[(E['Form / Instrument']=='Q-Interactive Ravlt') & (E['Unavailable']=='U')]['Variable / Field Name'])
qintdf2upload=qintdf2uploadrestricted.drop(columns=restrictedQ)#+['Qint'])

#check
qintdf2upload['PIN']=qintdf2upload.subject+"_"+qintdf2uploadrestricted.redcap_event
qintdf2upload.loc[qintdf2upload.PIN.isin(DNR)]
qintdf2upload.loc[qintdf2upload.PIN.isin(RAVLTissues)]
qintdf2upload.shape
qintdf2upload.drop_duplicates(subset=['subject','redcap_event']).shape
qintdf2upload.shape

qintdf2uploadfreeze=qintdf2upload.loc[qintdf2upload.PIN.isin(freezelist)]

qintdf2uploadrestricted.to_csv("AABC_Q-Interactive_Restricted" + date.today().strftime("%Y-%m-%d") + '.csv',index=False)
qintdf2upload.to_csv("AABC_Q-Interactive_" + date.today().strftime("%Y-%m-%d") + '.csv',index=False)
qintdf2uploadfreeze.to_csv("Freeze1_AABC_Q-Interactive_" + date.today().strftime("%Y-%m-%d") + '.csv',index=False)

box.upload_file("AABC_Q-Interactive_Restricted" + date.today().strftime("%Y-%m-%d") + '.csv', Rsnaps)
box.upload_file("AABC_Q-Interactive_" + date.today().strftime("%Y-%m-%d") + '.csv', Asnaps)
box.upload_file("Freeze1_AABC_Q-Interactive_" + date.today().strftime("%Y-%m-%d") + '.csv', freezefolder)

###########################################################
#REDCAP
aabcarms = redjson(tok=secret.loc[secret.source=='aabcarms','api_key'].reset_index().drop(columns='index').api_key[0])
aabc=getframe(struct=aabcarms,api_url=config['Redcap']['api_url10'])

rall=list(E.loc[(E['AABC Pre-Release File']=='AABC_RedCap_<date>.csv')]['Variable / Field Name'])
rchk=[]
checkboxvarbs = ['washu_studies', 'croms_sexorient', 'mock_complete_when', 'task_complete_when', 'sub81', 'missscan', 'nih_toolbox_instmissing', 'nihtoolbox_bugfix', 'nih_toolbox_multiassess', 'actigraphy_partial_1', 'actigraphy_partial_2', 'vms_partial_1', 'actigraphy_partial_4', 'tlbxwin_dups', 'consume_d1type', 'consume_caffeine_s1', 'consume_nicotine_s1', 'consume_d2type', 'consume_caffeine_s2', 'consume_nicotine_s2', 'consume_d3type', 'consume_caffeine_s3', 'consume_nicotine_s3', 'consume_d4type', 'consume_caffeine_s4', 'consume_nicotine_s4', 'consume_d5type', 'consume_caffeine_s5', 'consume_nicotine_s5', 'consume_d6type', 'consume_caffeine_s6', 'consume_nicotine_s6', 'diagnose', 'diagnose1', 'diagnose2']
for checkboxvars in checkboxvarbs:
    rchk=rchk+[c for c in list(aabc.columns) if checkboxvars+"___" in c]

rallchk=['redcap_event_name']+list([i for i in rall if i not in checkboxvarbs]+rchk)

rdrop=list(E.loc[(E['AABC Pre-Release File']=='AABC_RedCap_<date>.csv') & (E['Unavailable']=='U')]['Variable / Field Name'])

aabcidvisitsrestricted=idvisits(aabc,keepsies=[r for r in rallchk if r not in ['PIN']]) #PIN is not autoomatically a part of the DL
aabcidvisitsrestricted2=pd.merge(aabcidvisitsrestricted,inventorysnapshot[['subject','redcap_event']],on=['subject','redcap_event'],how='inner')
#reorder
aabcidvisitsrestricted2=aabcidvisitsrestricted2[['subject','redcap_event']+[i for i in aabcidvisitsrestricted2.columns if i not in ['subject','redcap_event']]]

dropcheckboxvarbs=['tlbxwin_dups','washu_studies']
droprchk=[]
for dropcheckboxvars in dropcheckboxvarbs:
    droprchk=droprchk+[c for c in list(aabc.columns) if dropcheckboxvars+"___" in c]
droprallchk=list([i for i in rdrop if i not in dropcheckboxvarbs]+droprchk)
subcols=[i for i in aabcidvisitsrestricted2 if i not in droprallchk]
aabcidvisits=aabcidvisitsrestricted2[subcols]
freezeaabcidvisits=aabcidvisits.loc[aabcidvisits.subject.isin(freezesubjects)].copy()

#replace -9999 with nothing
for i in ['bp_sitting_systolic','bp_sitting_diastolic','bp_standing_systolic','bp_standing_diastolic']:
    freezeaabcidvisits.loc[freezeaabcidvisits[i].isin(['-9999']),i]=''#[['subject','redcap_event',i]]

#ALREADY PULLED OUT THE SSAGA VARIABLES AND only set to restricted
#ALREADY changed the Inventory so that it wouldn't try to keep these variables
##isolate the SSAGA variables.
#allcols=list(aabcidvisits.columns)
#subssag=[c for c in SSAGAvars if c in allcols]
#subsubsag=[c for c in subssag if c != "subject"]
#subsubsag=[c for c in subsubsag if c != "redcap_event"]
#ssaga=aabcidvisits[['subject','redcap_event']+subsubsag]
#ssaga=ssaga.loc[ssaga.redcap_event.str.contains('V')]
#aabcidvisits=aabcidvisits.drop(columns=subsubsag).copy() #don't want to drop subject and redcap_event from export

#allrest=list(aabcidvisitsrestricted2)
#subssagrest=[t for t in SSAGAvars if t in allrest]
#subsubsagrest=[c for c in subssagrest if c != "subject"]
#subsubsagrest=[c for c in subsubsagrest if c != "redcap_event"]
#ssagarest=aabcidvisitsrestricted2[['subject','redcap_event']+subsubsagrest]
#ssagarest=ssagarest.loc[ssagarest.redcap_event.str.contains('V')]
#aabcidvisitsrestricted2=aabcidvisitsrestricted2.drop(columns=subsubsagrest).copy() #don't want to drop subject and redcap_event from export


aabcidvisitsrestricted2.to_csv("AABC_RedCap_Restricted" + date.today().strftime("%Y-%m-%d") + '.csv',index=False)
aabcidvisits.to_csv("AABC_RedCap_" + date.today().strftime("%Y-%m-%d") + '.csv',index=False)
freezeaabcidvisits.to_csv("Freeze1_AABC_RedCap_" + date.today().strftime("%Y-%m-%d") + '.csv',index=False)

aabcidvisits.shape
aabcidvisits.drop_duplicates(subset=['subject','redcap_event']).shape

box.upload_file("AABC_RedCap_Restricted" + date.today().strftime("%Y-%m-%d") + '.csv', Rsnaps)
box.upload_file("AABC_RedCap_" + date.today().strftime("%Y-%m-%d") + '.csv', Asnaps)
box.upload_file("Freeze1_AABC_RedCap_" + date.today().strftime("%Y-%m-%d") + '.csv', freezefolder)

##THIS ALREADY DONE AND NO NEW DATA COLLECTED
#ssagarest.to_csv("AABC_SSAGA_Restricted" + date.today().strftime("%Y-%m-%d") + '.csv',index=False)
#ssaga.to_csv("AABC_SSAGA_" + date.today().strftime("%Y-%m-%d") + '.csv',index=False)
#box.upload_file("AABC_SSAGA_" + date.today().strftime("%Y-%m-%d") + '.csv', Asnaps)

#Menopause adjudication (uploaded to PreRelease by hand - now create freeze copy)
straw=pd.read_excel(box.downloadFile(config['strawadj']), sheet_name='AABC_STRAW_List_2024-02-28')
straw['PIN']= straw['subject'] + '_' + straw['redcap_event']
print(straw.shape)
strawfreeze = straw.loc[(straw.PIN.isin(freezelist+HCAlist))]
print(strawfreeze.shape)
mvars=list(E.loc[(E['Form / Instrument']=='STRAW+10') & (~(E['Unavailable']=='U'))]['Variable / Field Name'])
fvars=[i for i in strawfreeze.columns if i in mvars]
strawfreeze[fvars].to_csv("Freeze1_AABC-HCA_Adjudicated-STRAW10_" + date.today().strftime("%Y-%m-%d") + '.csv',index=False)

box.upload_file("Freeze1_AABC-HCA_Adjudicated-STRAW10_" + date.today().strftime("%Y-%m-%d") + '.csv',freezefolder)

###########################################################
#TOOLBOX
rf2full=pd.read_csv(outp+"tempclean_TLBX_RAW.csv",low_memory=False)
rf2full['subject']=rf2full.PIN.str.split("_",1,expand=True)[0]
rf2full = rf2full.loc[~(rf2full.Inst.astype(str).str.upper().str.contains("PRACTICE"))]
rf2full = rf2full.loc[~(rf2full.Inst.astype(str).str.upper().str.contains("INSTRUCTIONS"))]
rf2full = rf2full.loc[~(rf2full.Inst.astype(str).str.upper().str.contains("AGES 3-7"))]
rf2full = rf2full.loc[~(rf2full.Inst.astype(str).str.upper().str.contains("AGES 3-9"))]

rf2full['redcap_event']=rf2full.PIN.str.split("_",1,expand=True)[1]
rlist=list(rf2full.PIN.unique())

dffull=pd.read_csv(outp+"tempclean_TLBX_SCORES.csv",low_memory=False)
dffull['subject']=dffull.PIN.str.split("_",1,expand=True)[0]
dffull = dffull.loc[~(dffull.Inst.astype(str).str.upper().str.contains("PRACTICE"))]
dffull = dffull.loc[~(dffull.Inst.astype(str).str.upper().str.contains("INSTRUCTIONS"))]
dffull = dffull.loc[~(dffull.Inst.astype(str).str.upper().str.contains("AGES 3-7"))]
dffull = dffull.loc[~(dffull.Inst.astype(str).str.upper().str.contains("AGES 3-9"))]

dffull['redcap_event']=dffull.PIN.str.split("_",1,expand=True)[1]
slist=list(dffull.PIN.unique())

rwlist=[value for value in rlist if value in slist]
rf2full=rf2full.loc[rf2full.PIN.isin(rwlist)]
dffull=dffull.loc[dffull.PIN.isin(rwlist)]

#RAW
rawvars=list(E.loc[(E['Form / Instrument']=='NIH-Toolbox-Raw File Column Descriptions')]['Variable / Field Name'])
rraw=list(E.loc[(E['Form / Instrument']=='NIH-Toolbox-Raw File Column Descriptions') & (E['Unavailable']=='U')]['Variable / Field Name'])
svars=list(E.loc[(E['Form / Instrument']=='NIH-Toolbox-Scores File Column Descriptions')]['Variable / Field Name'])
rs=list(E.loc[(E['Form / Instrument']=='NIH-Toolbox-Scores File Column Descriptions') & (E['Unavailable']=='U')]['Variable / Field Name'])


rf2full=rf2full.drop_duplicates()
len(rf2full.PIN.unique())
rf2full.shape
rf2fullrestricted=pd.merge(rf2full,inventorysnapshot[['PIN']],on=['PIN'],how='inner')
rf2fullrestricted=rf2fullrestricted.loc[~(rf2fullrestricted.PIN.isin(TLBXissues))][rawvars]
rf2fuller=rf2fullrestricted.drop(columns=rraw)
freezerfull=rf2fuller.loc[rf2fuller.PIN.isin(freezelist)]
len(rf2fullrestricted.PIN.unique())
len(freezerfull.PIN.unique())

rf2fullrestricted.to_csv("AABC_NIH-Toolbox-Raw_Restricted_" + date.today().strftime("%Y-%m-%d") + '.csv',index=False)
rf2fuller.to_csv("AABC_NIH-Toolbox-Raw_" + date.today().strftime("%Y-%m-%d") + '.csv',index=False)
freezerfull.to_csv("Freeze1_AABC_NIH-Toolbox-Raw_" + date.today().strftime("%Y-%m-%d") + '.csv',index=False)

##check
#rf2fullrestricted.loc[rf2fullrestricted.PIN.isin(DNR)]
#rf2fullrestricted.loc[rf2fullrestricted.PIN.isin(TLBXissues)]
#rf2fullrestricted.shape
#rf2fullrestricted.drop_duplicates().shape

box.upload_file("AABC_NIH-Toolbox-Raw_Restricted_" + date.today().strftime("%Y-%m-%d") + '.csv', Rsnaps)
box.upload_file("AABC_NIH-Toolbox-Raw_" + date.today().strftime("%Y-%m-%d") + '.csv', Asnaps)
box.upload_file("Freeze1_AABC_NIH-Toolbox-Raw_" + date.today().strftime("%Y-%m-%d") + '.csv', freezefolder)


#SCORES
dffull=dffull.drop_duplicates()
len(dffull.PIN.unique())
dffull.shape
dffullrestricted=pd.merge(dffull,inventorysnapshot[['PIN']],on=['PIN'],how='inner')
dffullrestricted=dffullrestricted.loc[~(dffullrestricted.PIN.isin(TLBXissues))]
len(dffullrestricted.PIN.unique())

dffullrestricted=dffullrestricted.loc[~(dffullrestricted.PIN.isin(TLBXissues))][svars]
dffuller=dffullrestricted.drop(columns=rs)
sfreezerfull=dffuller.loc[dffuller.PIN.isin(freezelist)]
len(dffullrestricted.PIN.unique())
len(sfreezerfull.PIN.unique())

dffullrestricted.shape
dffullrestricted.drop_duplicates().shape
dffuller.shape
sfreezerfull.shape

dffullrestricted.to_csv("AABC_NIH-Toolbox-Scores_Restricted_" + date.today().strftime("%Y-%m-%d") + '.csv',index=False)
dffuller.to_csv("AABC_NIH-Toolbox-Scores_" + date.today().strftime("%Y-%m-%d") + '.csv',index=False)
sfreezerfull.to_csv("Freeze1_AABC_NIH-Toolbox-Scores_" + date.today().strftime("%Y-%m-%d") + '.csv',index=False)

#check
dffullrestricted.loc[dffullrestricted.PIN.isin(DNR)]
dffullrestricted.loc[dffullrestricted.PIN.isin(TLBXissues)]

box.upload_file("AABC_NIH-Toolbox-Scores_Restricted_" + date.today().strftime("%Y-%m-%d") + '.csv', Rsnaps)
box.upload_file("AABC_NIH-Toolbox-Scores_" + date.today().strftime("%Y-%m-%d") + '.csv', Asnaps)
#box.upload_file("Freeze1_AABC_NIH-Toolbox-Scores_" + date.today().strftime("%Y-%m-%d") + '.csv', freezefolder)

########################################################################
#Cobra
Cobra=pd.read_csv(outp+"tempclean_Cobra_standard.csv")
Cobra['subject']=Cobra.PIN.str.split("_",1,expand=True)[0]
Cobra['redcap_event']=Cobra.PIN.str.split("_",1,expand=True)[1]

cvars=list(E.loc[(E['Form / Instrument']=='Actigraphy Data Summaries Produced By Cobra Lab')]['Variable / Field Name'])
RCOBRAS=list(E.loc[(E['Form / Instrument']=='Actigraphy Data Summaries Produced By Cobra Lab') & (E['Unavailable']=='U')]['Variable / Field Name'])

CobraRestricted=pd.merge(Cobra[cvars],inventorysnapshot[['PIN']],on="PIN",how='inner')
CobraRestricted=CobraRestricted[['subject','redcap_event','PIN']+[i for i in list(CobraRestricted.columns) if i not in ['subject','redcap_event','PIN']]]
#list of restricted variables: rcobras

CobraRestricted=CobraRestricted.loc[~(CobraRestricted.PIN.isin(Actissues))]
CobraRestricted=CobraRestricted.loc[~(CobraRestricted.PIN.isin(DNR))]
#other anomalies:
CobraRestricted=CobraRestricted.loc[~(CobraRestricted.NIGHTS==0.0)]
CobraRestricted=CobraRestricted.drop_duplicates(subset=['subject','redcap_event'])
Cobras=CobraRestricted.drop(columns=RCOBRAS)
FreezeCobras=Cobras.loc[Cobras.PIN.isin(freezelist)]

CobraRestricted.to_csv("AABC_Actigraphy-Summary_Restricted_" + date.today().strftime("%Y-%m-%d") + '.csv',index=False)
Cobras.to_csv("AABC_Actigraphy-Summary_" + date.today().strftime("%Y-%m-%d") + '.csv',index=False)
#FreezeCobras.to_csv("Freeze1_AABC_Actigraphy-Summary_" + date.today().strftime("%Y-%m-%d") + '.csv',index=False)

box.upload_file("AABC_Actigraphy-Summary_Restricted_" + date.today().strftime("%Y-%m-%d") + '.csv', Rsnaps)
box.upload_file("AABC_Actigraphy-Summary_" + date.today().strftime("%Y-%m-%d") + '.csv', Asnaps)
#box.upload_file("Freeze1_AABC_Actigraphy-Summary_" + date.today().strftime("%Y-%m-%d") + '.csv', freezefolder)

# waiting for confirmation of 'cleanliness' from Cobra's lab

# One time copy by "hand" for freeze:
#### Hotflash to Freeze
#### copy 'raw' xlsx for Actigraphy
### APOE and MEtabolites sent via SpecimenDerivatives.py
### MRS data copied by hand to preserve other xls tabs
### IDPs copied by hand

###############################################
#now get copies of HCA restricted REDCap, Q
hca_pre = box.list_of_files([str(config['hca_pre'])])
hca_restricted = box.list_of_files([str(config['hca_pre_restricted'])])
#whittle down the list of files in the Pre-Release folder to the most recent subset
hcafiles=pd.DataFrame.from_dict(hca_pre, orient='index')
hcafiles=hcafiles.loc[~((hcafiles.filename.str.upper().str.contains('DICTIONARY')) | (hcafiles.filename.str.upper().str.contains('ENCYCLOPEDIA')) | (hcafiles.filename.str.contains('HCA_Apoe-Isoforms')) | (hcafiles.filename.str.upper().str.contains('REDCAP')) | (hcafiles.filename.str.upper().str.contains('INVENTORY'))) ].copy()
hcafiles['datestamp']=hcafiles.filename.str.split('_',expand=True)[2]
hcafiles['datatype']=hcafiles.filename.str.split('_',expand=True)[1]
hcafiles=hcafiles.loc[(hcafiles.datestamp.str.contains('.csv')==True) & (hcafiles.datestamp.str.contains('-'))].copy()
hcafiles.datestamp=hcafiles.datestamp.str.replace('.csv','')
hcafiles.datestamp=pd.to_datetime(hcafiles.datestamp)
hcafiles=hcafiles.sort_values('datestamp',ascending=False)
hcafiles=hcafiles.drop_duplicates(subset='datatype',keep='first').copy()

hcaRfiles=pd.DataFrame.from_dict(hca_restricted, orient='index')
hcaRfiles=hcaRfiles.loc[hcaRfiles.filename.str.upper().str.contains('REDCAP')].copy()
hcaRfiles['datestamp']=hcaRfiles.filename.str.split('_',expand=True)[3]
hcaRfiles['datatype']=hcaRfiles.filename.str.split('_',expand=True)[1]
hcaRfiles=hcaRfiles.loc[(hcaRfiles.datestamp.str.contains('.csv')==True) & (hcaRfiles.datestamp.str.contains('-'))].copy()
hcaRfiles.datestamp=hcaRfiles.datestamp.str.replace('.csv','')
hcaRfiles.datestamp=pd.to_datetime(hcaRfiles.datestamp)
hcaRfiles=hcaRfiles.sort_values('datestamp',ascending=False)
hcaRfiles=hcaRfiles.drop_duplicates(subset='datatype',keep='first').copy()
hcafilesC=pd.concat([hcafiles,hcaRfiles],axis=0)

#these are the IDs/events of the V2 behavioral data-only that need to be dropped from HCA for the freeze or there will be two sets of behavioral data for V2
v2oopsexp=['HCA6686191_V2', 'HCA7296183_V2','HCA6686191_F2', 'HCA7296183_F2','HCA6686191_F3', 'HCA7296183_F3','HCA6686191_Covid', 'HCA7296183_Covid','HCA6686191_CR', 'HCA7296183_CR']

for i in list(hcafilesC.fileid):
    datatype = hcafilesC.loc[hcafilesC.fileid == i]['datatype'][0]
    if "Toolbox" in datatype:
        pass
    else:
        print("downloading", datatype, i, "...")
        dfile=pd.read_csv(box.downloadFile(i),low_memory=False)
        dfile['PIN']=dfile['subject']+'_'+dfile['redcap_event']
        print(dfile.shape)
        freezefile=dfile.loc[(dfile.subject.isin(HCAsubjects)) & (~(dfile.PIN.isin(v2oopsexp))) & (~(dfile.redcap_event.isin(['A','Covid','CR','F1','F2','F3'])))]
        print(freezefile.shape)
        #limit variables to those in Encyclopedia not "U"  non-covid variables
        rall=list(E.loc[(E['HCA Pre-Release File'].str.contains(datatype)) & (~(E['Unavailable']=='U')) & (~(E['Form / Instrument'].str.upper().str.contains("COVID")))]['Variable / Field Name'])
        #keep track of dropped variables
        rdrop = list(E.loc[(E['HCA Pre-Release File'].str.contains(datatype)) & ((E['Unavailable'] == 'U') | (E['Form / Instrument'].str.upper().str.contains("COVID"))) ]['Variable / Field Name'])
        #checkbox variables get expanded during export, so have to account for ___ in names, and remove their root from varlist
        #first find all of them
        chks=[i for i in freezefile.columns if "___" in i]
        chks1=[i.replace("___1","") for i in chks]
        chks2 = [i.replace("___2", "") for i in chks1]
        chks3 = [i.replace("___3", "") for i in chks2]
        chks4 = [i.replace("___4", "") for i in chks3]
        chks5 = [i.replace("___5", "") for i in chks4]
        chks6 = [i.replace("___6", "") for i in chks5]
        chks7 = [i.replace("___7", "") for i in chks6]
        chks8 = [i.replace("___8", "") for i in chks7]
        chks9 = [i.replace("___9", "") for i in chks8]
        chks10 = [i.replace("___10", "") for i in chks9]
        chks999 = [i.replace("___999", "") for i in chks10]
        checks = list(set(chks999))
        #now find the ones that were in the rdrop because will need to drop them from chks
        dropchks=[i for i in rdrop if i in checks]
        dchks = []
        for d in dropchks:
            dchks = dchks + [i for i in chks if d in i]
        keepchecks=[]
        extra=[]
        xdrop=[]
        keepchecks = [k for k in chks if k not in dchks]
        if datatype=='RedCap':
            extra=['redcap_event_name']
            xdrop=['site']
            #scrub inconsistent 999 values in bp and labs
            misslist=['bp_sitting','bp_standing','bld_core_d2ph','bld_core_d2pm','bld_core_p2fh','bld_core_p2fm','hba1c', 'hscrp','insulin','vitamind', 'albumin','alkphos_total','alt_sgpt','ast_sgot','calcium','chloride','co2content','creatinine','glucose','potassium','sodium','totalbilirubin','totalprotein','ureanitrogen','friedewald_ldl','hdl','cholesterol','triglyceride','ldl','estradiol','testosterone','lh','fsh']
            for j in misslist:
                freezefile.loc[freezefile[j].isin(['-9999','99999','9999']), i] = ''
        #these not in HCA
        if datatype=='Q-Interactive':
            xdrop='form'
            HCAQlist = list(freezefile.PIN.unique())
        if datatype=='PennCNP':
            HCAPennlist = list(freezefile.PIN.unique())
            xdrop=['DDISC.SV_1wk_20', 'DDISC.SV_2wk_20', 'DDISC.SV_1mo_20', 'DDISC.SV_6mo_20', 'DDISC.SV_1yr_20', 'DDISC.SV_3yr_20', 'DDISC.SV_1wk_100', 'DDISC.SV_2wk_100', 'DDISC.SV_1mo_100', 'DDISC.SV_6mo_100', 'DDISC.SV_1yr_100', 'DDISC.SV_3yr_100', 'DDISC.ExperimentName', 'DDISC.AUC_20', 'DDISC.AUC_100', 'DDISC.CompareMoney', 'DDISC.CompareMoneyDelay', 'DDISC.CompareDelay']
        rallchk=extra+list([i for i in rall if i not in checks and i not in dropchks and i not in xdrop])+keepchecks
        fullfreeze=freezefile[rallchk]
        fullfreeze=fullfreeze[['PIN','subject','redcap_event']+[i for i in fullfreeze.columns if i not in ['PIN','subject','redcap_event']]]
        fullfreeze.to_csv("Freeze1_HCA_" + datatype + "_" + date.today().strftime("%Y-%m-%d") + '.csv', index=False)
        if datatype=='RedCap':
            HCARed=fullfreeze.copy()
        print("Shape of file",fullfreeze.shape)
        #box.upload_file("Freeze1_HCA_"+datatype + "_" + date.today().strftime("%Y-%m-%d") + '.csv', freezefolder)

for i in list(hcafilesC.fileid):
    datatype = hcafilesC.loc[hcafilesC.fileid == i]['datatype'][0]
    #print(datatype)
    if "Toolbox" in datatype:
        print("downloading", datatype, i, "...")
        dfile = pd.read_csv(box.downloadFile(i), low_memory=False)
        dfile['PIN'] = dfile['subject'] + '_' + dfile['redcap_event']
        print("yes practice",dfile.shape)
        dfile=dfile.loc[~(dfile.Inst.astype(str).str.upper().str.contains("PRACTICE"))]
        dfile = dfile.loc[~(dfile.Inst.astype(str).str.upper().str.contains("PRACTICE"))]
        dfile = dfile.loc[~(dfile.Inst.astype(str).str.upper().str.contains("INSTRUCTIONS"))]
        dfile = dfile.loc[~(dfile.Inst.astype(str).str.upper().str.contains("AGES 3-7"))]
        dfile = dfile.loc[~(dfile.Inst.astype(str).str.upper().str.contains("AGES 3-9"))]
        print("no practice",dfile.shape)
        Evars=list(E.loc[(E['Form / Instrument']==datatype+' File Column Descriptions') & (E.Unavailable != 'U')]['Variable / Field Name'])
        freezefile = dfile.loc[(dfile.subject.isin(HCAsubjects)) & (~(dfile.PIN.isin(v2oopsexp))) & (~(dfile.redcap_event.isin(['A', 'Covid', 'CR', 'F1', 'F2', 'F3'])))]
        HCATLBXlist = list(freezefile.PIN.unique())
        print(len(HCATLBXlist))
        freezefile=freezefile[Evars].copy()
        freezefile.to_csv("Freeze1_HCA_"+datatype + "_" + date.today().strftime("%Y-%m-%d") + '.csv',index=False)
        box.upload_file("Freeze1_HCA_"+datatype + "_" + date.today().strftime("%Y-%m-%d") + '.csv', freezefolder)
    print("*********")



##################################################
# NEXT: Completeness Inventory - All and Freeze  HCA and AABC
# first concat HCA and AABC inventory main vars (PIN, subject,'redcap_event')
# then merge by subject, redcap event
# then by subject (genotype)
"""
    -BIGGESTTotals2
    -qintdf2upload
    -dffuller
    -Cobras
    -HotInvent
    -Mets
    -Apoe
    -mrs
    IDPstruct
    IDPfunc
    CogComp
    straw adjudicated
    penncnp
    redcap breakdown aabcidvisits and hcaredcap
"""

#Need to grab representative indicators from HCA REDCap, which doesn't have form_complete variables.
#Harmlist=['iecr2nd_c','moca_sum','iihandwr','med_num','lan_know','ipaq_category','iadl1','mstrl1a','straw_code','trail1','weight','bld_draw','alt_sgpt','neo_n','psqi_global','gales_worst','asr1','oasr_ppl3','dental1','protocol_order','satisfaction1']
Harmlist=['moca_sum','iihandwr','med_num','lan_know','ipaq_category','iadl1','mstrl1a','straw_code','trail1','weight','alt_sgpt','neo_n','psqi_global','gales_worst','asr1','oasr_ppl3','dental1']
Harmlist2=['moca_complete','handedness_complete','medication_review_complete','language_experience_and_proficiency_questionnaire_complete','international_physical_activity_questionnaire_bloc_complete','lawton_instrumental_activities_of_daily_living_iad_complete','menstrual_cycle_block1_complete', 'straw10_block1_complete','trail_making_scores_complete','vital_signs_external_measures_complete','lab_results_complete','neo_complete','pittsburgh_sleep_quality_index_psqi_block1_complete','the_life_events_scale_gales_block2_complete','achenbach_adult_selfreport_asr_block2_complete','achenbach_older_adult_selfreport_oasr_block2_complete','dental_work_questionnaire_complete']
renames=dict(zip(Harmlist,Harmlist2))
HCARedHarm=HCARed[['subject','redcap_event']+Harmlist].copy()
HCARedHarm=HCARedHarm.rename(columns=renames)
HCARedHarm.replace('', pd.NA, inplace=True)
for i in Harmlist2:
    HCARedHarm.loc[HCARedHarm[i].isna()==False,i]='YES'

hcainventory=pd.read_csv(box.downloadFile(config['hcainventoryR']))
hcainventory['PIN']=hcainventory.subject+"_"+hcainventory.redcap_event
hcainventory=hcainventory.loc[~(hcainventory.IntraDB=='CCF_PCMP_ITK')]

#set aside for later, since this is a HCA-AABC combined variable, like genotypes
peds=hcainventory[['subject','pedid']].drop_duplicates()

#do some clean up so that it can get merged with the aabc inventory
hcainventory=hcainventory[['site',  'redcap_event',
       'redcap_event_name', 'REDCap_id', 'subject', 'race',
       'ethnic_group', 'M/F', 'event_age',
       'Curated_SSAGA', 'PIN']]
hcainventory=hcainventory.rename(columns={'Curated_SSAGA':'SSAGA','REDCap_id':'study_id','site':'Site'})
hcainventory.SSAGA.value_counts()
hcainventory.loc[hcainventory.SSAGA.isin(['NE','NE PM','YES BUT','SEE V1','SEE V2']),'SSAGA']=''
hcainventory=hcainventory.loc[(~(hcainventory.PIN.isin(v2oopsexp))) & (~(hcainventory.redcap_event.isin(['Covid','CR','A'])))]
hcainventory['study']='HCA'
#merge in the indicator variables.
hcainventory=hcainventory.merge(HCARedHarm,on=['subject','redcap_event'],how='left')

aabcinventory=inventorysnapshot.copy()
aabcinventory['study']='AABC'
aabcinventory['PIN']=aabcinventory.subject+"_"+aabcinventory.redcap_event

Inventory=pd.concat([hcainventory,aabcinventory],axis=0)
Inventory['event_age']=Inventory.event_age.astype(float).round(1)

#Inventory will only have the visits for now, since FU and Covid aren't harmonized in HCA
Inventory=Inventory.loc[Inventory.redcap_event.isin(['V1','V2','V3','V4'])]

#define cohorts
v2list=list(set([i.replace('_V2','') for i in HCAlist if 'V2' in i]))
Inventory.loc[(Inventory.PIN.isin(HCAlist)),'Cohort']='HCA Cross'
Inventory.loc[(Inventory.PIN.isin(HCAlist)) & (Inventory.subject.isin(v2list)),'Cohort']='HCA Long'
Inventory.loc[(~(Inventory.PIN.isin(HCAlist))) & ((Inventory.redcap_event_name.str.contains("arm_1")) | (Inventory.redcap_event_name.str.contains("arm_2")) |(Inventory.redcap_event_name.str.contains("arm_3")) |(Inventory.redcap_event_name.str.contains("arm_4"))) ,'Cohort']='AABC A'
Inventory.loc[(~Inventory.PIN.isin(HCAlist)) & ((Inventory.redcap_event_name.str.contains("arm_5")) | (Inventory.redcap_event_name.str.contains("arm_6")) |(Inventory.redcap_event_name.str.contains("arm_7")) |(Inventory.redcap_event_name.str.contains("arm_8"))) ,'Cohort']='AABC B'
Inventory.loc[(~Inventory.PIN.isin(HCAlist)) & ((Inventory.redcap_event_name.str.contains("arm_9")) | (Inventory.redcap_event_name.str.contains("arm_10")) |(Inventory.redcap_event_name.str.contains("arm_11")) |(Inventory.redcap_event_name.str.contains("arm_12"))) ,'Cohort']='AABC C'
Inventory.Cohort.value_counts(dropna=False)

############################################################
#grab indicators for all the rest of the datatypes (AABC REDCap has them built in, but HCA REDCap had to be grabbed above)
Q=qintdf2upload[['PIN']].drop_duplicates(subset='PIN').copy()
Q['RAVLT']='YES'
print(Q.shape)
print(len(Q.PIN.unique()))
QH=pd.DataFrame(HCAQlist)
QH.columns=['PIN']
QH['RAVLT']='YES'
Qall=pd.concat([Q,QH],axis=0).drop_duplicates(subset=['PIN'])
Inventory=Inventory.merge(Qall,on='PIN',how='left')

TLBX=dffuller[['PIN']].drop_duplicates(subset='PIN').copy()
TLBX['TLBX']='YES'
print(TLBX.shape)
print(len(TLBX.PIN.unique()))
HTLBX=pd.DataFrame(HCATLBXlist)
HTLBX.columns=['PIN']
HTLBX['TLBX']='YES'
TLBXall=pd.concat([TLBX,HTLBX],axis=0).drop_duplicates(subset=['PIN'])
Inventory=Inventory.merge(TLBXall,on='PIN',how='left')

IntraDB=getPCP(intradb=intradb,project='AABC_STG',pipeline='StructuralPreprocessing')
IntraDB['IntraDB']=IntraDB.project
IntraDB=IntraDB.loc[IntraDB.validated==True].copy()
IntraDB=IntraDB[['PIN']]

HIntraDB=getPCP(intradb=intradb,project='CCF_HCA_STG',pipeline='StructuralPreprocessing')
HIntraDB['IntraDB']=HIntraDB.project
HIntraDB=HIntraDB.loc[HIntraDB.validated==True].copy()
HIntraDB=HIntraDB[['PIN']]

HCAAABC=pd.concat([IntraDB,HIntraDB],axis=0)
HCAAABC['Bulk_Imaging']='YES'
Inventory=Inventory.merge(HCAAABC,on='PIN',how='left')

Totals=BIGGESTTotals2[['PIN']].drop_duplicates(subset='PIN').copy()
Totals['ASA24_Totals']='YES'
Inventory=Inventory.merge(Totals,on='PIN',how='left')

Act=Cobras[['PIN']].copy()
Act['Actigraphy_Cobra']='YES'
print(Act.shape)
print(len(Act.PIN.unique()))
Inventory=Inventory.merge(Act,on='PIN',how='left')

#capture IDs of Hot data in Freeze
HotFiles=pd.read_csv(outp+"temp_Hotties.csv")[['PIN']]
HotInvent=pd.merge(HotFiles,inventorysnapshot[['PIN']],on="PIN",how='inner')
HotInvent=HotInvent.loc[~(HotInvent.PIN.isin(HotFlashissues))]
Hot=HotInvent[['PIN']].copy()
Hot['Raw_VMS']='YES'
Inventory=Inventory.merge(Hot,on='PIN',how='left')

straw['Menopause_Adjud']="YES"
strawslim=straw[['PIN','Menopause_Adjud']].copy()
Inventory=Inventory.merge(strawslim,on='PIN',how='left')

### HERE
import numpy as np
Mets=pd.read_csv(outp+'AABC-HCA_Metabolites-AD-Biomarkers_2024-03-21.csv',low_memory=False)
M=Mets[['PIN','AD_Biomarkers','Metabolites']].copy()
M=M.replace(1.0,'YES')#['Metab_AD_Biomark']='YES'
M=M.replace(np.nan,'')#['Metab_AD_Biomark']='YES'
Inventory=Inventory.merge(M,on='PIN',how='left')

mrs = pd.read_excel(box.downloadFile(config['mrsRatoi']), sheet_name='conc_tCr')
mrs['PIN'] = mrs['subject'] + '_' + mrs['redcap_event']
print(mrs.shape)
mrsfreeze = mrs.loc[(mrs.PIN.isin(freezelist))]
print(mrsfreeze.shape)
MR7=mrs[['PIN']].copy()
MR7['MRS_7T']='YES'
Inventory=Inventory.merge(MR7,on='PIN',how='left')

Penn=pd.DataFrame(HCAPennlist)
Penn.columns=['PIN']
Penn['PennCNP']='YES'
Inventory=Inventory.merge(Penn,on='PIN',how='left')

#IDPs
rfMRI=pd.read_csv(outp+"rfMRI_REST_FullAmplitudes.csv")[['x___']].rename(columns={'x___':'PIN'})
rfMRI['rfMRI_IDPs']='YES'
Inventory=Inventory.merge(rfMRI,on='PIN',how='left')
T1T2=pd.read_csv(outp+"Cortical_Areal_Myelin.csv")[['x___']].rename(columns={'x___':'PIN'})
T1T2['T1T2_IDPs']='YES'
Inventory=Inventory.merge(T1T2,on='PIN',how='left')

#last ones need to be merged in by subject, not PIN
Apoe=pd.read_csv(outp+'AABC-HCA_APOE-PRS_2024-03-21.csv',low_memory=False)
A=Apoe[['subject']].copy()
A['APOE']='YES'
Inventory=Inventory.merge(A,on='subject',how='left')

gwasfamfile=outp+"HCA_imputed_geno0.02_final.fam"
gwas=pd.DataFrame(list(pd.read_csv(gwasfamfile,sep='\t',header=None)[0].unique())) #949
gwas.columns=['subject']
gwas['GWAS']='YES'
Inventory=Inventory.merge(gwas,on='subject',how='left')

Inventory=Inventory.merge(peds,on='subject',how='left')
colorder=['Cohort','study','Site','pedid','PIN', 'subject','redcap_event',
       'race', 'ethnic_group', 'M/F', 'event_age',  'RAVLT',
       'TLBX', 'Bulk_Imaging', 'MRS_7T','rfMRI_IDPs', 'T1T2_IDPs','Raw_VMS',
       'AD_Biomarkers', 'Metabolites','APOE', 'GWAS',
       'PennCNP',  'SSAGA', 'ASA24_Totals', 'Actigraphy_Cobra',
       'moca_complete', 'neo_complete','pittsburgh_sleep_quality_index_psqi_block1_complete',
       'lawton_instrumental_activities_of_daily_living_iad_complete',
       'international_physical_activity_questionnaire_bloc_complete',
       'menstrual_cycle_block1_complete', 'straw10_block1_complete','Menopause_Adjud',
       'greene_climacteric_scale_block1_complete',
       'international_personality_inventory_pool_ipip_bloc_complete',
       'the_life_events_scale_gales_block2_complete',
       'duke_university_religion_index_durel_block3_complete',
       'perceived_everyday_discrimination_block3_complete',
       'neighborhood_disorderneighborhood_social_cohesion_complete',
       'ongoing_chronic_stressors_scale_block3_complete',
       'barriers_to_healthcare_checklist_block3_complete',
       'access_to_healthcare_phenx_block3_complete',
       'global_health_block3_complete', 'brief_cope_block4_complete',
       'cesd_block4_complete',
       'memory_function_questionnaire_mfq_block4_complete',
       'substance_use_reporting_complete',
       'medication_review_complete', 'medical_history_complete',
       'handedness_complete', 'demographics_complete',
       'language_experience_and_proficiency_questionnaire_complete',
       'dental_work_questionnaire_complete', 'trail_making_scores_complete',
       'achenbach_adult_selfreport_asr_block2_complete',
       'achenbach_alerts_asr_block2_complete',
       'achenbach_older_adult_selfreport_oasr_block2_complete',
       'actigraphy_complete', 'vasomotor_symptom_device_vms_complete',
       'vital_signs_external_measures_complete',
       'lab_results_complete',
       'achenbach_alerts_oasr_block2_complete' ]#'positive_and_negative_affect_schedule_complete',

Inventory=Inventory[colorder]
#Medical history form in SSAGA for HCA
Inventory.loc[Inventory.SSAGA=='YES','medical_history_complete']='YES'


#Now break it down by completed REDCap instrument
#Has score
scores=['moca_complete','neo_complete','pittsburgh_sleep_quality_index_psqi_block1_complete',
 'international_physical_activity_questionnaire_bloc_complete','cesd_block4_complete',
 'lawton_instrumental_activities_of_daily_living_iad_complete','international_personality_inventory_pool_ipip_bloc_complete']
scoresrename=['MOCA','NEO','PSQI','IPAQ','CESD','IADL','IPIP']
#no score (need attention from experts)
noscore=['menstrual_cycle_block1_complete','straw10_block1_complete','greene_climacteric_scale_block1_complete',
 'achenbach_adult_selfreport_asr_block2_complete','achenbach_alerts_asr_block2_complete','achenbach_older_adult_selfreport_oasr_block2_complete','achenbach_alerts_oasr_block2_complete','the_life_events_scale_gales_block2_complete','duke_university_religion_index_durel_block3_complete',
 'neighborhood_disorderneighborhood_social_cohesion_complete','perceived_everyday_discrimination_block3_complete',
 'ongoing_chronic_stressors_scale_block3_complete','access_to_healthcare_phenx_block3_complete', 'barriers_to_healthcare_checklist_block3_complete',
 'global_health_block3_complete','medication_review_complete','substance_use_reporting_complete','medical_history_complete',
 'handedness_complete','demographics_complete','language_experience_and_proficiency_questionnaire_complete',
 'brief_cope_block4_complete','memory_function_questionnaire_mfq_block4_complete','dental_work_questionnaire_complete',
 'trail_making_scores_complete','lab_results_complete',
 'actigraphy_complete','vasomotor_symptom_device_vms_complete',	'vital_signs_external_measures_complete']#'positive_and_negative_affect_schedule_complete',
noscore_rename=['Menstrual Cycle','STRAW10','GREENE','ASR','Achenbach Alerts','OASR','OASR Achenbach Alerts','GALES','DUREL','NDSC','PED',
                'Ongoing Stress','Access to Health','Barriers to Health','Global Health','Medications','Substance Use Visit','Med Hx',
                'Handedness','Demographics','LEAP','COPE','MFQ','Dental Work','Trails','Core Labs',
                'Actigraphy Diary','VMS Diary','Vitals']#PANAS - is in the followup and will be brought in for the next freeze
scoreszip=dict(zip(scores,scoresrename))
noscoreszip=dict(zip(noscore,noscore_rename))

Inventory=Inventory.rename(columns=scoreszip)
Inventory=Inventory.rename(columns=noscoreszip)
Inventory[scoresrename+noscore_rename]=Inventory[scoresrename+noscore_rename].astype(str).replace('2','YES').replace('0','').replace('1','').replace('nan','')
###### PPPPPPPPP #####

#FREEZE 1
Inventory['Freeze1_Nov2023']=''
#InventoryRestricted['event_date']=pd.to_datetime(InventoryRestricted.event_date)
#freezelist=list(InventoryRestricted.loc[(InventoryRestricted.redcap_event.isin(['V1','V2','V3','V4'])) & (InventoryRestricted['event_date']<"2023-11-01")]['study_id'])
Inventory.loc[Inventory.PIN.isin(freezelist),"Freeze1_Nov2023"]='YES'

print(Inventory.loc[Inventory.Freeze1_Nov2023=='YES']['redcap_event'].value_counts())
Inventory=Inventory.drop_duplicates(subset=['subject','redcap_event'])

#InventoryRestricted.to_csv("AABC_Inventory_Restricted_" + date.today().strftime("%Y-%m-%d") + '.csv',index=False)
#box.upload_file("AABC_Inventory_Restricted_" + date.today().strftime("%Y-%m-%d") + '.csv', Rsnaps)
#InventoryRestricted.drop(columns=['event_date']).to_csv("AABC_Inventory_" + date.today().strftime("%Y-%m-%d") + '.csv',index=False)
Inventory.to_csv("AABC-HCA_Completeness_Inventory_" + date.today().strftime("%Y-%m-%d") + '.csv',index=False)
box.upload_file("AABC-HCA_Completeness_Inventory_" + date.today().strftime("%Y-%m-%d") + '.csv', Asnaps)

FreezeInventory=Inventory.loc[Inventory.PIN.isin(HCAlist+freezelist)].drop(columns=['Freeze1_Nov2023'])
FreezeInventory.to_csv("Union-Freeze1_AABC-HCA_Completeness_Inventory_" + date.today().strftime("%Y-%m-%d") + '.csv',index=False)
#box.upload_file("Union-Freeze1_AABC-HCA_Completeness_Inventory_" + date.today().strftime("%Y-%m-%d") + '.csv', '250512318481')

#generate stats for power point presentations
#All Completeness
print("----------------------")
print("All Available Data")
Inventoryunique=Inventory[['redcap_event','subject']].drop_duplicates()
print('Total Unique Subjects:',len(Inventoryunique.subject.unique()))
print(Inventory.redcap_event.value_counts())
ImgUnique=Inventory.loc[Inventory.Bulk_Imaging=='YES']
print('Total Unique Imaging Subjects:',len(ImgUnique.subject.unique()))
print(ImgUnique.redcap_event.value_counts())
dtypes=['Bulk_Imaging','MRS_7T','rfMRI_IDPs', 'T1T2_IDPs','Raw_VMS','RAVLT',
       'TLBX',  'AD_Biomarkers', 'Metabolites','APOE', 'GWAS',
       'PennCNP',  'SSAGA', 'ASA24_Totals', 'Actigraphy_Cobra']
for i in dtypes+scoresrename+noscore_rename:
    try:
        print(i,":",pd.DataFrame(Inventory[i].value_counts()).loc['YES'][0])
    except:
        print(i,": 0")
print("----------------------")

#Union FreezeInventory
print("----------------------")
print("Union Freeze")
UnionFreezeInventoryunique=FreezeInventory[['redcap_event','subject']].drop_duplicates()
print('Total Unique Subjects:',len(UnionFreezeInventoryunique.subject.unique()))
print(FreezeInventory.redcap_event.value_counts())
ImgUnique=FreezeInventory.loc[FreezeInventory.Bulk_Imaging=='YES']
print('Total Unique Imaging Subjects:',len(ImgUnique.subject.unique()))
print(ImgUnique.redcap_event.value_counts())
dtypes=['Bulk_Imaging','MRS_7T','rfMRI_IDPs', 'T1T2_IDPs','Raw_VMS','RAVLT',
       'TLBX',  'AD_Biomarkers', 'Metabolites','APOE', 'GWAS',
       'PennCNP',  'SSAGA', 'ASA24_Totals', 'Actigraphy_Cobra']
for i in dtypes+scoresrename+noscore_rename:
    try:
        print(i,":",pd.DataFrame(FreezeInventory[i].value_counts()).loc['YES'][0])
    except:
        print(i,": 0")
print("----------------------")

#HCAFreeze
print("----------------------")
print("HCA data in the Freeze")
HCAFreezeInventory=Inventory.loc[Inventory.PIN.isin(HCAlist)].drop(columns=['Freeze1_Nov2023'])
UnionFreezeInventoryunique=HCAFreezeInventory[['redcap_event','subject']].drop_duplicates()
print('Total Unique Subjects:',len(UnionFreezeInventoryunique.subject.unique()))
print(HCAFreezeInventory.redcap_event.value_counts())
ImgUnique=HCAFreezeInventory.loc[HCAFreezeInventory.Bulk_Imaging=='YES']
print('Total Unique Imaging Subjects:',len(ImgUnique.subject.unique()))
print(ImgUnique.redcap_event.value_counts())
dtypes=['Bulk_Imaging','MRS_7T','rfMRI_IDPs', 'T1T2_IDPs','Raw_VMS','RAVLT',
       'TLBX',  'AD_Biomarkers', 'Metabolites','APOE', 'GWAS',
       'PennCNP',  'SSAGA', 'ASA24_Totals', 'Actigraphy_Cobra']
for i in dtypes+scoresrename+noscore_rename:
    try:
        print(i,":",pd.DataFrame(HCAFreezeInventory[i].value_counts()).loc['YES'][0])
    except:
        print(i,": 0")
print("----------------------")

#AABCFreeze
print("----------------------")
print("AABC Data in the Freeze")
AABCFreezeInventory=Inventory.loc[Inventory.PIN.isin(freezelist)].drop(columns=['Freeze1_Nov2023'])
UnionFreezeInventoryunique=AABCFreezeInventory[['redcap_event','subject']].drop_duplicates()
print('Total Unique Subjects:',len(UnionFreezeInventoryunique.subject.unique()))
print(AABCFreezeInventory.redcap_event.value_counts())
ImgUnique=AABCFreezeInventory.loc[AABCFreezeInventory.Bulk_Imaging=='YES']
print('Total Unique Imaging Subjects:',len(ImgUnique.subject.unique()))
print(ImgUnique.redcap_event.value_counts())
dtypes=['Bulk_Imaging','MRS_7T','rfMRI_IDPs', 'T1T2_IDPs','Raw_VMS','RAVLT',
       'TLBX',  'AD_Biomarkers', 'Metabolites','APOE', 'GWAS',
       'PennCNP',  'SSAGA', 'ASA24_Totals', 'Actigraphy_Cobra']
for i in dtypes+scoresrename+noscore_rename:
    try:
        print(i,":",pd.DataFrame(AABCFreezeInventory[i].value_counts()).loc['YES'][0])
    except:
        print(i,": 0")
print("----------------------")

#upload the Freeze specific
Efreeze=E.loc[~((E['Form / Instrument'].str.upper().str.contains('COVID')) | (E['Form / Instrument'].str.upper().str.contains('PANAS')))].copy()
Efreeze['Freeze1 HCA Source']='Freeze1_' + Efreeze['HCA Pre-Release File']
Efreeze['Freeze1 AABC Source']='Freeze1_'+ Efreeze['AABC Pre-Release File']
Efreeze.drop(columns=['HCA Pre-Release File','AABC Pre-Release File'])
Efreeze.to_csv("Union-Freeze1_AABC-HCA_Encyclopedia_" + date.today().strftime("%Y-%m-%d") + '.csv',index=False)
#box.upload_file("Union-Freeze1_AABC-HCA_Encyclopedia_" + date.today().strftime("%Y-%m-%d") + '.csv', '250512318481')
