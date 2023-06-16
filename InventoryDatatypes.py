import pandas as pd
from ccf.box import LifespanBox
import re
import collections
from functions import *
from config import *
from datetime import date

## get configuration files
outp="/Users/petralenzini/work/Behavioral/AABC/AABC_Behavioral_QC/AABC_Behavioral_QC/tmp/"
config = LoadSettings()
secret=pd.read_csv(config['config_files']['secrets'])
box = LifespanBox(cache="./tmp")

## get the HCA inventory for ID checking with AABC
pathp=box.downloadFile(config['hcainventory'])

#get current version variable mask from BOX (for excluding variables just prior to sending snapshots to PreRelease for investigator access)
Asnaps=161956162589
Rsnaps=203445756892
a=box.downloadFile(config['variablemask'])
rdrop=getlist(a,'AABC-ARMS-DROP')
rrest=getlist(a,'AABC-ARMS-RESTRICTED')
rraw=getlist(a,'TLBX-RAW-RESTRICTED')
rscore=getlist(a,'TLBX-SCORES-RESTRICTED')
restrictedQ=getlist(a,'Q-RESTRICTED')
restrictedATotals=getlist(a,'ASA24-Totals')
restrictedAResp=getlist(a,'ASA24-Resp')
restrictedATS=getlist(a,'ASA24-TS')
restrictedAINS=getlist(a,'ASA24-INS')
restrictedATNS=getlist(a,'ASA24-TNS')
restrictedAItems=getlist(a,'ASA24-Items')
rcobras=getlist(a,'COBRAS')

#get ids
ids=pd.read_csv(pathp)
hcaids=ids.subject.drop_duplicates()
#for later use in getting the last visit for each participant in HCA so that you can later make sure that person is starting subsequent visit and not accidentally enrolled in the wrong arm
hca_lastvisits=ids[['subject','redcap_event']].loc[ids.redcap_event.isin(['V1','V2'])].sort_values('redcap_event').drop_duplicates(subset='subject',keep='last')


#########################################################################################
#PHASE 0 TEST IDS AND ARMS
aabcarms = redjson(tok=secret.loc[secret.source=='aabcarms','api_key'].reset_index().drop(columns='index').api_key[0])
hcpa = redjson(tok=secret.loc[secret.source=='hcpa','api_key'].reset_index().drop(columns='index').api_key[0])
aabcreport = redreport(tok=secret.loc[secret.source=='aabcarms','api_key'].reset_index().drop(columns='index').api_key[0],reportid='51031')

#download the inventory report from AABC for comparison
aabcinvent=getframe(struct=aabcreport,api_url=config['Redcap']['api_url10'])

#trying to set study_id from config file, but have been sloppy...there are instances where the actual subject_id has been coded below
study_id=config['Redcap']['datasources']['aabcarms']['redcapidvar']

#slim selects just the registration event (V0) because thats where the ids and legacy information is kept.
slim=aabcinvent[['study_id','redcap_event_name',study_id,'legacy_yn','site','v0_date']].loc[(aabcinvent.redcap_event_name.str.contains('register'))]

#compare aabc ids against hcaids and whether legacy information is properly accounted for (e.g. legacy variable flags and actual event in which participannt has been enrolled.
fortest=pd.merge(hcaids,slim,left_on='subject',right_on=study_id,how="outer",indicator=True)
#fortest._merge.value_counts()
legacyarms=['register_arm_1','register_arm_2','register_arm_3','register_arm_4','register_arm_5','register_arm_6','register_arm_7','register_arm_8']

# First batch of flags: Look for legacy IDs that don't actually exist in HCA
ft=fortest.loc[(fortest._merge=='right_only') & ((fortest.legacy_yn=='1')|(fortest.redcap_event_name.isin(legacyarms)))]
#remove the TEST subjects -- probably better to do this first, but sigh.
ft=ft.loc[~((ft[study_id]=='')|(ft[study_id].str.upper().str.contains('TEST')))]
qlist1=pd.DataFrame()
if not ft.empty:
    ft['reason']='Subject found in AABC REDCap Database with legacy indications whose ID was not found in HCP-A list'
    ft['issueCode']='AE1001'
    ft['datatype']='REDCap'
    ft['code']='RED'
    qlist1=ft[['subject_id', 'study_id', 'redcap_event_name', 'site','reason','code','v0_date','datatype']]
    for s in list(ft[study_id].unique()):
        print('CODE RED :',s,': Subject found in AABC REDCap Database with legacy indications whose ID was not found in HCP-A list')

#2nd batch of flags: if legacy v1 and enrolled as if v3 or v4 or legacy v2 and enrolled v4
ft2=fortest.loc[(fortest._merge=='both') & ((fortest.legacy_yn != '1')|(~(fortest.redcap_event_name.isin(legacyarms))))]
qlist2=pd.DataFrame()
if not ft2.empty:
    ft2['reason']='Subject found in AABC REDCap Database with legacy indications whose ID was not found in HCP-A list'
    ft2['code']='RED'
    ft2['issueCode'] = 'AE1001'
    ft2['datatype']='REDCap'
    qlist2 = ft2[['subject_id', 'study_id', 'redcap_event_name', 'site','reason','code','v0_date','datatype']]
    for s2 in list(ft2[study_id].unique()):
        print('CODE RED :',s2,': Subject found in AABC REDCap Database with an ID from HCP-A study but no legacyYN not checked')

#if legacy v1 and enrolled as if v3 or v4 or legacy v2 and enrolled v4
#get last visit
hca_lastvisits["next_visit"]=''
#idvisits rolls out the subject ids to all visits. get subects current visit for comparison with last visit
aabcidvisits=idvisits(aabcinvent,keepsies=['study_id','redcap_event_name','site','subject_id','v0_date','event_date'])
sortaabc=aabcidvisits.sort_values(['study_id','redcap_event_name'])
sortaabcv=sortaabc.loc[~(sortaabc.redcap_event_name.str.contains('register'))]
sortaabcv.drop_duplicates(subset=['study_id'],keep='first')

#add 1 to last visit from HCA
hca_lastvisits.next_visit=hca_lastvisits.redcap_event.str.replace('V','').astype('int') +1
hca_lastvisits["next_visit2"]="V"+hca_lastvisits.next_visit.astype(str)
hca_lastvisits2=hca_lastvisits.drop(columns=['redcap_event','next_visit'])

#check that current visit in AABC is the last visit in HCA + 1
check=pd.merge(hca_lastvisits2,sortaabcv,left_on=['subject','next_visit2'],right_on=['subject','redcap_event'],how='outer',indicator=True)
check=check.loc[check._merge !='left_only']
wrongvisit=check.loc[check._merge=='right_only']
wrongvisit=wrongvisit.loc[~(wrongvisit.redcap_event.isin(['AP']))]#,'v1_inperson_arm_10','v1_inperson_arm_12']))]
wrongvisit=wrongvisit.loc[wrongvisit.next_visit2.isnull()==False]
qlist3=pd.DataFrame()
if not wrongvisit.empty:
    wrongvisit['reason']='Subject found in AABC REDCap Database initiating the wrong visit sequence (e.g. V3 insteady of V2'
    wrongvisit['code']='RED'
    wrongvisit['issueCode'] = 'AE1001'
    wrongvisit['datatype']='REDCap'
    qlist3 = wrongvisit[['subject', 'study_id', 'redcap_event_name', 'site','reason','code','v0_date','event_date','datatype']]
    qlist3=qlist3.rename(columns={'subject':'subject_id'})
    for s3 in list(wrongvisit['subject'].unique()):
        if s3 !='':
            print('CODE RED (if HCA6911778 ignore) :',s3,': Subject found in AABC REDCap Database initiating the wrong visit sequence (e.g. V3 insteady of V2')
            qlist3=qlist3.loc[~(qlist3.subject_id=='HCA6911778')].copy()

# check to make sure that the subject id is not missing.
missingsubids=aabcinvent.loc[(aabcinvent.redcap_event_name.str.contains('register')) & (aabcinvent[study_id]=='')].copy()
qlist4=pd.DataFrame()
if not missingsubids.empty:
    missingsubids['reason']='Subject ID is MISSING in AABC REDCap Database Record with study id'
    missingsubids['code']='RED'
    missingsubids['issueCode'] = 'AE1001'
    missingsubids['datatype']='REDCap'
    qlist4 = missingsubids[['subject_id', 'study_id', 'redcap_event_name', 'site','reason','code','v0_date','event_date','datatype']]
    for s4 in list(missingsubids.study_id.unique()):
        print('CODE RED : Subject ID is MISSING in AABC REDCap Database Record with study id:',s4)

#test subjects that need to be deleted
tests=aabcinvent.loc[(aabcinvent[study_id].str.upper().str.contains('TEST'))][['study_id',study_id,'redcap_event_name']]
qlist5=pd.DataFrame()
if not tests.empty:
    tests['reason']='HOUSEKEEPING : Please delete test subject.  Use test database when practicing'
    tests['code']='HOUSEKEEPING'
    tests['datatype']='REDCap'
    tests['issueCode'] = 'AE6001'
    qlist5 = tests[['subject_id', 'study_id', 'redcap_event_name', 'site','reason','code','v0_date','event_date','datatype']]
    for s5 in list(tests[study_id].unique()):
        print('HOUSEKEEPING : Please delete test subject:', s5)

#########################################################################################
###concatenate Phase 0 flags for REDCap key variables
Q0=pd.DataFrame(columns=['subject_id', 'study_id', 'redcap_event_name', 'site','reason','issue_code','code','v0_date','event_date'])
try:
    Q1 = concat(*[Q0,qlist1,qlist2,qlist3,qlist5])#qlist4
except:
    Q1=pd.DataFrame(columns=['subject_id', 'study_id', 'redcap_event_name', 'site','reason','issue_code','code','v0_date','event_date'])
#########################################################################################


#########################################################################################
# PHASE 1 Test that all dataypes expected are present
# Get the REDCap AABC inventory (which may or may not agree with the reality of data found):

inventoryaabc=idvisits(aabcinvent,keepsies=list(aabcinvent.columns))
inventoryaabc['PIN']=inventoryaabc.subject+"_"+inventoryaabc.redcap_event
print('test subjects:',inventoryaabc.loc[(inventoryaabc.subject_id.str.upper().str.contains('TEST'))])

inventoryaabc = inventoryaabc.loc[~(inventoryaabc.subject_id.str.upper().str.contains('TEST'))].copy()
#Q data grabber turned into a cron job via Qscratch and /Users/petralenzini/cron/runcron_AABC_QC.sh

#QC checks
qintreport = redreport(tok=secret.loc[secret.source=='qint','api_key'].reset_index().drop(columns='index').api_key[0],reportid='51037')
qintdf2=getframe(struct=qintreport,api_url=config['Redcap']['api_url10'])
#invq=qintdf2[['id', 'site', 'subjectid','visit','q_unusable']].copy()
qintdf2['redcap_event']="V"+qintdf2.visit
qintdf2['Qint']='YES'
qintdf2=qintdf2.loc[~(qintdf2.q_unusable=='1')].copy()
dups2=qintdf2.loc[qintdf2.duplicated(subset=['subjectid','visit'])]

q0=pd.DataFrame()
if dups2.shape[0]>0:
    print("Duplicate Q-interactive records")
    print(dups2[['subjectid','visit']])
    q0=dups2.copy()
    q0['datatype']='RAVLT'
    q0['reason']='Duplicate Q-interactive records'
    q0['code']='ORANGE'
    q0['issueCode']='AE5001'

inventoryaabc2=pd.merge(inventoryaabc,qintdf2.drop(columns=['site']),left_on=['subject','redcap_event'],right_on=['subjectid','redcap_event'],how='outer',indicator=True)
q1=pd.DataFrame()
if inventoryaabc2.loc[inventoryaabc2._merge=='right_only'].shape[0] > 0 :
    print("The following ID(s)/Visit(s) are not found in the main AABC-ARMS Redcap.  Please investigate")
    print(inventoryaabc2.loc[inventoryaabc2._merge=='right_only'][['subjectid','redcap_event','site']])
    q1=inventoryaabc2.loc[inventoryaabc2._merge=='right_only'][['subjectid','redcap_event','site']].rename(columns={'subjectid':'subject'})
    q1['reason']='Subject with Q-int data but ID(s)/Visit(s) are not found in the main AABC-ARMS Redcap.  Please look for typo or naming convention problem and forward correction instructions and ticket to Petra'
    q1['code']='ORANGE'
    q1['issueCode']='AE1001'
    q1['datatype']='RAVLT'

inventoryaabc3=inventoryaabc2.loc[inventoryaabc2._merge!='right_only'].drop(columns=['_merge'])

missingQ=inventoryaabc3.loc[(inventoryaabc3.redcap_event_name.str.contains('v')) & (~(inventoryaabc3.Qint=='YES')) & (~(inventoryaabc3.ravlt_collectyn=='0'))][['subject_id','study_id','subject','redcap_event','site','event_date','ravlt_collectyn']]
q2=pd.DataFrame()
if missingQ.shape[0]>0:
    print("Q-interactive cannot be found for")
    print(missingQ)
    q2=missingQ.copy()
    q2['reason']='MISSING, MISNAMED, or MALFORMED Q-interactive data for this subject/visit'
    q2['code']='ORANGE'
    q2['issueCode']='AE4001'
    q2['datatype']='RAVLT'

Q0=pd.DataFrame(columns=['subject_id', 'study_id', 'redcap_event_name', 'site','reason','issueCode','code','v0_date','event_date','datatype'])
Q2=pd.concat([Q0,q0,q1,q2],axis=0)
Q2['subject_id']=Q2.subject


# NOW FOR TOOLBOX. ############################################################################
# # 1. grab partial files from intraDB
# # 2. QC (after incorporating patches)
# # 3. generate tickets and send to JIra if don't already exist
# # 4. send tickets that arent identical to ones already in Jira
# # 5. concatenate legit data (A scores file and a Raw file, no test subjects or identical duplicates -- no 'Narrow' or 'Registration' datasets)
# # 6. create and send snapshot of patched data to BOX after dropping restricted variables

rf2=pd.read_csv(outp+"temp_TLBX_RAW.csv",low_memory=False)

rf2=rf2.loc[~(rf2.PIN.str.upper().str.contains('TEST'))]
rf2=rf2.loc[~(rf2.PIN.str.upper().str.contains('HCA1111111_CR'))]
rf2=rf2.loc[~(rf2.PIN.str.upper()=='ABC123')]
rf2=rf2.loc[~(rf2.PIN.str.upper()=='HCA9926201')]
rf2=rf2.loc[~(rf2.PIN.str.upper().str.contains('PRACTICE'))]
rf2=rf2.loc[~(rf2.PIN.str.contains('No line'))]

fixtypos=inventoryaabc.loc[inventoryaabc.nih_toolbox_upload_typo!=''][['subject','redcap_event','nih_toolbox_upload_typo']]
fixtypos['PIN']=fixtypos.subject+'_'+fixtypos.redcap_event
fixes=dict(zip(fixtypos.nih_toolbox_upload_typo, fixtypos.PIN))
rf2.PIN=rf2.PIN.replace(fixes)
#peek for remaining wierdness
rf2.PIN.unique()
###HERE###
#keep track of the full dataset so that you can drop anything with issues before sync
rf2full=rf2.copy()
rf2full=rf2full.drop_duplicates()

#drop duplicates for purpose of generating QC flags.
rf2=rf2.drop_duplicates(subset='PIN').copy()


#NOW THE SCORED DATA
#import moved to cron
dffull=pd.read_csv(outp+"temp_TLBX_Scores.csv")

dffull=dffull.loc[~(dffull.PIN.str.upper().str.contains('TEST'))]
dffull=dffull.loc[~(dffull.PIN.str.upper().str.contains('HCA1111111_CR'))]
dffull=dffull.loc[~(dffull.PIN.str.upper()=='ABC123')]
dffull=dffull.loc[~(dffull.PIN.str.upper()=='HCA9926201')]
dffull=dffull.loc[~(dffull.PIN.str.upper().str.contains('PRACTICE'))]
dffull=dffull.loc[~(dffull.PIN.str.contains('No line'))]

dffull.PIN=dffull.PIN.replace(fixes)
dffull=dffull.drop_duplicates()
#merge with patch fixes (i.e. delete duplicate specified in visit summary)
# This is a single fix... need to generalized to all instruments and their corresponding dupvars:
# -->HCA8596099_V3 has 2 assessments for Words in Noise - add patch note"
iset=inventoryaabc
dffull=filterdupass('NIH Toolbox Words-In-Noise Test Age 6+ v2.1','tlbxwin_dups_v2',iset,dffull)
print(dffull.shape)
dffull=filterdupass('NIH Toolbox 2-Minute Walk Endurance Test Age 3+ v2.0','walkendur_dups',iset,dffull)
print(dffull.shape)
dffull=filterdupass('NIH Toolbox 4-Meter Walk Gait Speed Test Age 7+ v2.0','tlbx4walk_dups',iset,dffull)
print(dffull.shape)

#dffull=filterdupass('NIH Toolbox 2-Minute Walk Endurance Test Age 3+ v2.0','psmt_dups',iset,dffull)

rf2full=filterdupass('NIH Toolbox Words-In-Noise Test Age 6+ v2.1','tlbxwin_dups_v2',iset,rf2full)
print(rf2full.shape)
rf2full=filterdupass('NIH Toolbox 2-Minute Walk Endurance Test Age 3+ v2.0','walkendur_dups',iset,rf2full)
print(rf2full.shape)
rf2full=filterdupass('NIH Toolbox 4-Meter Walk Gait Speed Test Age 7+ v2.0','tlbx4walk_dups',iset,rf2full)
print(rf2full.shape)

#find any non-identical duplicated Assessments still in data after patch
dupass=dffull.loc[dffull.duplicated(subset=['PIN','Inst'],keep=False)][['PIN','Assessment Name','Inst']]

#TURN THIS INTO A TICKET
duptix=dupass.drop_duplicates(subset='PIN').copy()
if duptix.shape[0]>0:
    duptix['code']='ORANGE'
    duptix['issueCode'] = 'AE5001'
    duptix['datatype'] = 'TLBX'
    duptix['reason'] = "Non-Identical Duplicate Found for "+duptix.Inst
    i3=inventoryaabc[['subject', 'study_id', 'redcap_event_name', 'redcap_event','event_date']].copy()
    i3["PIN"]=i3.subject+'_'+i3.redcap_event
    duptix=pd.merge(duptix,i3,on='PIN',how='left')

#before sending the dffull and rf2full to BOX, need to drop the duplicates from rf2full

#QC check:
#Either scored or raw is missing in format expected:
formats=pd.merge(dffull.PIN.drop_duplicates(),rf2,how='outer',on='PIN',indicator=True)[['PIN','_merge']]
issues=formats.loc[~(formats._merge=='both')]
t1=pd.DataFrame()
if issues.shape[0]>0:
    t1=issues.copy()
    t1['code']='ORANGE'
    t1['issueCode']='AE5001'
    t1['datatype']='TLBX'
    t1['reason']="Raw or Scored data not found (make sure you didn't export Narrow format)"
    print("Raw or Scored data not found (make sure you didn't export Narrow format)")
    print(issues[['PIN']])

#find cases where PIN was reused (e.g. PIN is the same but date more than 3 weeks different
#extend this section by pivoting and subtracting dates, then searching for diffs>60 days.
dffull['Date']=dffull.DateFinished.str.split(' ', expand=True)[0]
catchdups=dffull.loc[~(dffull.Date.isnull()==True)]
c=catchdups.drop_duplicates(subset=['PIN','Date'])[['PIN','Date']]
c.loc[c.duplicated(subset='PIN',keep=False)]

#add subject and visit
df2=dffull.drop_duplicates(subset='PIN').copy()
df2['redcap_event']=df2.PIN.str.split("_",expand=True)[1]
df2['subject']=df2.PIN.str.split("_",expand=True)[0]
df2['redcap_event']=df2.PIN.str.split("_",expand=True)[1]
df2['TLBX']='YES'

#now merge with inventory
inventoryaabc4=pd.merge(inventoryaabc,df2[['subject','redcap_event','TLBX','PIN','site']].rename(columns={'site':'siteT'}),on=['subject','redcap_event'],how='outer',indicator=True)

#find PINS with length greater than 10
tlong=pd.DataFrame()
dflong=df2.drop_duplicates(subset='PIN').copy()
if dflong.loc[dflong.PIN.str.len() > 13].shape[0] > 0 :
    tlong=dflong.loc[dflong.PIN.str.len() > 13].copy()
    tlong['reason']='TOOLBOX PIN typos need to be addressed in the visit summary?'
    tlong['code']='ORANGE'
    tlong['issueCode']='AE1001'
    tlong['datatype']='TLBX'
    print("Too. Legit. Too legit 2 quit. Break it down: The following TOOLBOX PIN typos need to be addressed in the visit summary")
    print(tlong.PIN)

#find toolbox records that aren't in AABC - typos are one thing...legit ids are bad because don't know which one is right unless look at date, which is missing for cog comps
#turn this into a ticket
t2=pd.DataFrame()
if inventoryaabc4.loc[inventoryaabc4._merge=='right_only'].shape[0] > 0 :
    t2=inventoryaabc4.loc[inventoryaabc4._merge=='right_only'].copy()
    t2['reason']='TOOLBOX PINs are not found in the main AABC-ARMS Redcap.  Typo?'
    t2['code']='ORANGE'
    t2['issueCode']='AE1001'
    t2['datatype']='TLBX'
    print("The following TOOLBOX PINs are not found in the main AABC-ARMS Redcap.  Please investigate")
    print(inventoryaabc4.loc[inventoryaabc4._merge=='right_only'][['PIN','subject','redcap_event','site','siteT']])
t2b=pd.DataFrame()
if df2.loc[df2.PIN.str.len()>13].shape[0] >0 :
    t2b=df2.loc[df2.PIN.str.len()>13].copy()
    t2b['reason']='TOOLBOX PINs are not found in the main AABC-ARMS Redcap.  Typo?'
    t2b['code']='ORANGE'
    t2b['issueCode']='AE1001'
    t2b['datatype']='TLBX'
    print("The following TOOLBOX PINs are not found in the main AABC-ARMS Redcap.  Please investigate")
    print(df2.loc[df2.PIN.str.len()>13][['PIN','subject','redcap_event','site']])

#t2b=t2b.loc[~t2b.PIN.str.contains('HCA855')]
inventoryaabc4=inventoryaabc4.loc[inventoryaabc4._merge!='right_only'].drop(columns=['_merge'])

# Look for missing IDs
missingT=inventoryaabc4.loc[(inventoryaabc4.redcap_event_name.str.contains('v')) & (~(inventoryaabc4.TLBX=='YES'))]
t3=pd.DataFrame()
if missingT.shape[0]>0:
    t3=missingT.copy()
    t3['reason']='Missing TLBX data'
    t3['code']='ORANGE'
    t3['issueCode']='AE2001'
    t3['datatype']='TLBX'
    print("TLBX cannot be found for")
    print(missingT[['subject','redcap_event','site','event_date','nih_toolbox_collectyn']])

#T=pd.concat([duptix,t1,t2b,t3])[['subject','study_id','redcap_event_name','redcap_event', 'event_date','PIN', 'reason', 'code','issueCode','datatype']]
T=pd.concat([duptix,t1,t2,t3,t2b,tlong])[['subject','study_id','redcap_event_name','redcap_event', 'event_date','PIN', 'reason', 'code','issueCode','datatype']]
#merge with site num from inventory
T=pd.merge(T,inventoryaabc4[['subject','redcap_event','site']],on=['subject','redcap_event'],how='left')


######################################################################


### NOW For ASA 24 ######################################################################
# ORDER
# 1. scan for data (here just looking for existende)
# 2. QC ids (after incorporating patches and translating ASAID into AABC id)
# 3. generate tickets and send to JIra if don't already exist
#  4. send tickets that arent identical to ones already in Jira
# # # 5. just dump all legit data to BOX (transform to be defined later) after patching, dropping restricted variables, and merging in subject and redcap_event
# # # 6. create and send snapshot of patched data to BOX after dropping restricted variables

#just read in latest stuff from cron-job -- is in ./tmp directory
BIGGESTTotals=pd.read_csv(outp+'temp_Totals.csv')
BIGGESTItems=pd.read_csv(outp+'temp_Items.csv')
BIGGESTResp=pd.read_csv(outp+'temp_Resp.csv')
BIGGESTTS=pd.read_csv(outp+'temp_TTS.csv')
BIGGESTTNS=pd.read_csv(outp+'temp_TNS.csv')
BIGGESTINS=pd.read_csv(outp+'temp_INS.csv')

AD=BIGGESTTotals[['PIN','UserName']].rename(columns={'PIN':'PIN_perBox'}).copy()
AD['asa24id']=AD.UserName#=pd.DataFrame(anydata,columns=['asa24id'])
AD=AD.drop_duplicates()
AD['ASA24']='YES'

#missings
inventoryaabc5=pd.merge(inventoryaabc,AD,on='asa24id',how='outer',indicator=True)
missingAD=inventoryaabc5.loc[(inventoryaabc5._merge != 'right_only') & (inventoryaabc5.redcap_event_name.str.contains('v')) & (~(inventoryaabc5.ASA24=='YES'))]
missingAD=missingAD.loc[~(missingAD.asa24yn=='0')]
a1=pd.DataFrame()
if missingAD.shape[0]>0:
    print("ASA24 cannot be found for")
    print(missingAD[['subject','redcap_event','site','event_date','asa24yn','asa24id']])
    a1=missingAD.copy()
    a1['reason']='Unable to locate ASA24 id in Redcap or ASA24 data in Box for this subject/visit'
    a1['code']='GREEN'
    a1['datatype']='ASA24'
    a1['issueCode']='AE2001'
    a1['subject_id']=a1['subject']
a1=a1[['subject_id','subject', 'study_id', 'redcap_event','redcap_event_name', 'site','reason','code','issueCode','v0_date','event_date','datatype']]
#a1 is concatenated later with other q2 codes
#typo PIN PINbox is different that PINRedcap
missmatch=inventoryaabc5.loc[(inventoryaabc5.PIN != inventoryaabc5.PIN_perBox) & (inventoryaabc5.asa24id != "")]
a11=pd.DataFrame()
if missmatch.shape[0]>0:
    print("ASA24 id matches to different study ids in Box vs Redcap")
    print(missmatch[['subject','redcap_event','site','event_date','asa24yn','asa24id','PIN','PIN_perBox']])
    a11=missingAD.copy()
    a11['reason']='ASA24 id associated with different study ids in Box vs Redcap.  Check for typos or missingness'
    a11['code']='GREEN'
    a11['datatype']='ASA24'
    a11['issueCode']='AE2001'
    a11['subject_id']=a1['subject']
a11=a11[['subject_id','subject', 'study_id', 'redcap_event','redcap_event_name', 'site','reason','code','issueCode','v0_date','event_date','datatype']]

#typo same PIN but different asa24id
missmatch2=pd.merge(inventoryaabc,AD,how='inner',left_on='PIN',right_on='PIN_perBox')
mm=missmatch2.loc[missmatch2.UserName!=missmatch2.asa24id_y]
a111=pd.DataFrame()
if mm.shape[0]>0:
    print("Subject associated with 2 ASA24 ids in Box vs Redcap")
    print(mm[['subject','redcap_event','site','event_date','asa24yn','asa24id','PIN_perBox']])
    a111=mm.copy()
    a111['reason']='ASA id associated with different subject ids in Box and Redcap'
    a111['code']='GREEN'
    a111['datatype']='ASA24'
    a111['issueCode']='AE2001'
    a111['subject_id']=a1['subject']
    a111=a111[['subject_id','subject', 'study_id', 'redcap_event','redcap_event_name', 'site','reason','code','issueCode','v0_date','event_date','datatype']]

#################################################################################
#ACTIGRAPHY
### for now, this is basically the same protocol as for ASA24
#move scan to cron

actdatat=pd.read_csv(outp+"temp_actigraphy.csv")
actdata=actdatat.PIN.to_list()

#Duplicates?
if [item for item, count in collections.Counter(actdata).items() if count > 1] != '':
    print('Duplicated Actigraphy Record Found:',[item for item, count in collections.Counter(actdata).items() if count > 1])

ActD=pd.DataFrame(actdata,columns=['PIN'])
ActD['Actigraphy']='YES'
#actigraphy_upload_typoyn
fixtypos=inventoryaabc.loc[inventoryaabc.actigraphy_upload_typo!=''][['subject','redcap_event','actigraphy_upload_typo']]
fixtypos['PIN']=fixtypos.subject+'_'+fixtypos.redcap_event
fixes=dict(zip(fixtypos.actigraphy_upload_typo, fixtypos.PIN))
ActD.PIN=ActD.PIN.replace(fixes)

inventoryaabc2=pd.merge(inventoryaabc,ActD,on='PIN',how='left')

#Missing  : add exception for partial missing
missingAct=inventoryaabc2.loc[(inventoryaabc2.redcap_event_name.str.contains('v')) & (~(inventoryaabc2.Actigraphy=='YES'))]
missingAct.shape
#people that have partial data and it isknown that actigraphy device data are missing
partials=missingAct.loc[ (missingAct.actigraphy_collectyn == '2' ) & (missingAct.actigraphy_partial_1___1.astype('int')==0)][['PIN','actigraphy_collectyn','subject','actigraphy_partial_1___1']]
#don't want to include these guys in the list
missingAct=missingAct.loc[~missingAct.PIN.isin(list(partials.PIN))]
#drop the ones that are definitely nos, too.
missingAct=missingAct.loc[~(missingAct.actigraphy_collectyn == '0' )].copy()




a2=pd.DataFrame()
if missingAct.shape[0]>0:
    print("Actigraphy cannot be found for")
    print(missingAct[['subject','redcap_event','site','event_date','actigraphy_collectyn']])
    a2=missingAct.copy()
    a2['reason']='Unable to locate Actigraphy data in Box for this subject/visit'
    a2['code']='YELLOW'
    a2['datatype']='Actigraphy'
    a2['issueCode']='AE4001'
    a2['subject_id']=a2['subject']
a2=a2[['subject_id','subject','redcap_event', 'study_id', 'redcap_event_name', 'site','reason','code','issueCode','v0_date','event_date','datatype']]

#NOW Do the Consolidated Actigraphy stuff from Cobras
# file = config['NonQBox']['Cobras'][studyshort]
studyshort='WUSM'
Cobra=pd.DataFrame()
for studyshort in ['WUSM','MGH']:
    box.downloadFile(config['NonQBox']['Cobras'][studyshort])
    WUCobra=pd.read_excel("./tmp/"+studyshort+"_actig_all.xlsx",sheet_name='Sheet1')
    x=list(WUCobra.columns)
    z=[y.upper() for y in x]
    WUCobra.columns=z
    WUCobra=WUCobra.rename(columns={"PARTICIPANT ID":"PIN"})
    WUCobra[['PIN','NIGHTS']]
    WUCobra=WUCobra.loc[WUCobra.NIGHTS.isnull()==False].copy()
    Cobra=pd.concat([Cobra,WUCobra])

#For now just capture typos, since collection status is checked elsewhere
#use fixes from above
Cobra.PIN=Cobra.PIN.replace(fixes)

AllCobra=pd.merge(Cobra,inventoryaabc[['PIN']],how='outer',on='PIN',indicator=True)
#Bad IDs
print("TRACK DOWN BAD IDS FROM COBRAS:",AllCobra.loc[AllCobra._merge=='left_only'])

#merge Cobra with later inventorysnapshot for upload


#MOCA SPANISH  #############################################################
## no data yet

############################################################################
# Psychopy
# ORDER
# 1. scan for data in Box (here just looking for existence) and in IntraDB
# 2. QC ids (after incorporating patches)
# 3. generate an tickets and send to JIra if don't already exist
# 4. DONT dump or snapshot.  Leave data in IntraDB.


# maybe a different method of correcting typos is needed.  IntraDB needs to have correct information unless there is a patch somewhere.
# so need to create a patch for psychopy .. usually just one of the scans had a typo (e.g. vismotor but not resting state)

#box scan moved to cron
anydata=pd.read_csv(outp+"temp_psychopy.csv")
anydata.columns=['subject','redcap_event','scan','fname','']
anydata.loc[~(anydata.subject.isin(["HCA1234567"]))]

PSY=anydata[['subject','redcap_event']].drop_duplicates().copy()
checkIDB=anydata[['subject','redcap_event','scan']].copy()
checkIDB['PIN_AB']=checkIDB.subject+'_'+checkIDB.redcap_event + '_'+checkIDB.scan
ci=checkIDB.drop_duplicates(subset='PIN_AB')

#sent intradb scan to psychopy
df=pd.read_csv(outp+"temp_psychintradb.csv")
df.columns = ['PIN_AB']
df.PIN_AB=df.PIN_AB.str.replace('t','').str.strip()

psymiss=pd.merge(ci,df, on='PIN_AB',how='outer',indicator=True).drop_duplicates()

p1=pd.DataFrame()
if psymiss.loc[psymiss._merge=='left_only'].shape[0]>0:
    p1 = pd.DataFrame(psymiss.loc[psymiss._merge=='left_only'].PIN_AB.unique())
    p1=p1.loc[~((p1[0].isnull()==True) | ((p1[0].str.contains("HCA678899_V3"))==True) | ((p1[0].str.contains("HCA996201_V3"))==True) | ((p1[0].str.contains("HCA752775_V3"))==True))].copy()
    newp1=pd.DataFrame([item for item in list(p1[0]) if ('_A' in str(item) or '_B' in str(item))])
    newp1['code']='ORANGE'
    newp1['issueCode']='AE4001'
    newp1['datatype']='PsychoPy'
    newp1['reason']='A and/or B Psychopy Data Found in Box but not IntraDB'
print('A and/or B Psychopy Data Found in Box but not IntraDB')
print(newp1[0])

print('psychopy in IntraDB but not in Box -- not turning into tickets for now because bug in merge')
for i in psymiss.loc[psymiss._merge=='right_only'].PIN_AB.unique():
    print(i)

p2=pd.DataFrame()

pwho=pd.DataFrame()
p=pd.concat([newp1,p2])
#p=p2.copy()
if p.shape[0]>0:#,columns='PIN_AB')
    p['PIN']=p[0].str[:13]
    p['PIN_AB']=p[0]
    p['subject_id']=p[0].str[:10]
    p['subject']=p[0].str[:10]
    pwho=pd.merge(inventoryaabc.loc[inventoryaabc.redcap_event.astype('str').str.contains('V')].drop(columns=['subject_id','subject']),p,on='PIN',how='right')
    pwho=pwho[['subject','subject_id', 'study_id', 'redcap_event','redcap_event_name', 'site','reason','issueCode','code','v0_date','event_date','PIN_AB','datatype']]

#dont worry about duplicates in IntraDB - these will be filtered.
#find subjects in AABC but not in IntraDB or BOX
PSY2=psymiss.drop_duplicates(subset='subject')[['subject','redcap_event']]
PSY2['Psychopy']='YES'
inventoryaabc7=pd.merge(inventoryaabc,PSY2,on=['subject','redcap_event'],how='left')
missingPY=inventoryaabc7.loc[(inventoryaabc7.redcap_event_name.str.contains('v')) & (~(inventoryaabc7.Psychopy=='YES'))].copy()
dropm=inventoryaabc7.loc[(inventoryaabc7.missscan4=='1') | (inventoryaabc7.missscan4=='2') | (inventoryaabc7.scan_collectyn=='0')][['PIN']]
missingPY=missingPY.loc[~(missingPY.PIN.isin(list(dropm.PIN.unique())))].copy()
missingPY['subject_id']=missingPY.subject
#missingPY=missingPY.loc[~(missingPY.asa24yn=='0')]
peepy=pd.DataFrame()
if missingPY.shape[0]>0:
    peepy=missingPY
    print("PsyhoPy cannot be found in BOX or IntraDB for ")
    print(missingPY[['subject','redcap_event','site','event_date','Psychopy']])
    peepy['reason']='PsychoPy cannot be found in BOX or IntraDB'
    peepy['code']='ORANGE'
    peepy['datatype']='PsychoPy'
    peepy['issueCode']='AE4001'

P=pd.concat([pwho,peepy])
#IntraDB ID
P=P[['subject','redcap_event','study_id', 'site','reason','code','issueCode','v0_date','event_date','datatype']]
P=P.drop_duplicates()
#no psychopy data:
P=P.loc[~((P.subject=="HCA6487286") & (P.redcap_event=='V3'))]

##################################################################################
#HOT FLASH DATA
#NOTE: Maki's group to do the scoring and stuff.  We are just checking for IDs and typos
HotFiles=pd.read_csv(outp+"temp_Hotties.csv")
Hotties=pd.merge(inventoryaabc,HotFiles,on='PIN',how='outer',indicator=True)

Typos=Hotties.loc[(Hotties._merge=='right_only')].copy()[['PIN']]
#typo ids:
Hot1=pd.DataFrame()
if Typos.shape[0]>0:
    Hot1=Typos.copy()
    print("TYPOS  ",Hot1.PIN)
    Hot1['reason']='Subject in Box cannot be found in REDCap'
    Hot1['code']='ORANGE'
    Hot1['datatype']='HotFlash'
    Hot1['issueCode']='AE4001'

#should have data for 40 - 59 women
#check against visit summary expectations
Female=Hotties.loc[Hotties.sex=='2'].copy()[['subject']]
H=Hotties.loc[(Hotties.age_visit !='') & (Hotties.subject.isin(list(Female.subject.unique())))]
H2=H.loc[(H._merge=='left_only') & (H.redcap_event_name.str.contains('v')) & (H.age_visit.astype('float') >= 40) & (H.age_visit.astype('float') <=59)]
#These are the guys we need to locate
Hot2=H2.loc[~((H2.vms_collectyn=='0') | (H2.vms_collectyn=='3'))]#[['PIN','subject','vms_collectyn','vms_partial_1___0','vms_partial_1___1','vms_partial_1___2']]
Hot2b=pd.DataFrame()
if Hot2.shape[0]>0:
    Hot2b=Hot2.copy()
    print("HotFlash Data cannot be found in BOX  ")
    print(Hot2[['subject','redcap_event','site','event_date']])
    Hot2b['reason']='HotFlash Data cannot be found in BOX - upload or fix visit summary'
    Hot2b['code']='ORANGE'
    Hot2b['datatype']='HotFlash'
    Hot2b['issueCode']='AE4001'


###################################################################################
# To DO: Forgot to CHECK FOR BUNK IDS IN PSYCHOPY AND ACTIGRAPHY
###################################################################################


###################################################################################
# NOW CHECK key REDCap AABC variables for completeness (counterbalance, inventory completeness, age, bmi and soon to be more)
# inventory_complete
pd.set_option('display.width', 1000)
pd.options.display.width=1000

cb=inventoryaabc.loc[(inventoryaabc.redcap_event_name.str.contains('register')) & (inventoryaabc.counterbalance_2nd=='')][['site','study_id','redcap_event','redcap_event_name','subject','v0_date','passedscreen']]
if cb.shape[0]>0:
    print("Current Counterbalance Ratio:\n", inventoryaabc.counterbalance_2nd.value_counts())
    print("Currently Missing Counterbalance:", cb)
    cb['reason']='Currently Missing Counterbalance'
    cb['code']='PINK'
    cb['datatype']='REDCap'
    cb['issueCode']='AE3001'
#QC
C=cb[['subject','redcap_event','study_id', 'site','reason','code','issueCode','v0_date','datatype']]
C.rename(columns={'v0_date':'event_date'})


summv=inventoryaabc.loc[inventoryaabc.redcap_event_name.astype('str').str.contains('v')][['study_id','site','subject','redcap_event','visit_summary_complete','event_date']]
summv=summv.loc[~(summv.visit_summary_complete=='2')]
if summv.shape[0]>0:
    summv['code']='GREEN'
    summv['issueCode']='AE2001'
    summv['datatype']='REDCap'
    summv['reason']='Visit Summary Incomplete'
    summv=summv[['subject','redcap_event','study_id', 'site','reason','code','issueCode','event_date','datatype']]
    print("Visit Summary Incomplete:\n",summv)

agev=inventoryaabc7.loc[inventoryaabc7.redcap_event_name.astype('str').str.contains('v')][['redcap_event', 'study_id', 'site','subject','redcap_event_name','age_visit','event_date','v0_date']]
ag=agev.loc[agev.age_visit !='']
agemv=ag.loc[(ag.age_visit.astype('float')<=40) | (ag.age_visit.astype('float')>=95 )].copy()
if agemv.shape[0]>0:
    print("AGE OUTLIERS:\n",agemv)
    agemv['code']='RED'
    agemv['issueCode']='AE7001'
    agemv['datatype']='REDCap'
    agemv['reason']='Age outlier. Please double check DOB and Event Date'
    agemv=agemv[['subject','redcap_event','study_id', 'site','reason','issueCode','code','event_date','v0_date','datatype']]

aa=agev.loc[agev.age_visit=='']
ab=agev.loc[~(agev.age_visit=='')]
ageav=pd.concat([aa,ab.loc[(ab.age_visit.astype(float).isnull()==True)]])
if ageav.shape[0]>0:
    print("Missing Age:\n",ageav)
    ageav['code']='RED'
    ageav['datatype']='REDCap'
    ageav['issueCode']='AE3001'
    ageav['reason']='Missing Age. Please check DOB and Event Date'
    ageav=ageav[['subject','redcap_event','study_id', 'site','reason','issueCode','code','event_date','v0_date','datatype']]

#calculate BMI: weight (lb) / [height (in)]2 x 703
bmiv=inventoryaabc.loc[inventoryaabc.redcap_event_name.astype('str').str.contains('v')][['bmi','redcap_event','subject','study_id', 'site','event_date','height_outlier_jira','weight_outlier_jira', 'height_missing_jira']].copy()
#outliers
#add in check against the extreme value confirmation and then relax the extremes

a=bmiv.loc[bmiv.bmi !=''].copy()
a=a.loc[(a.bmi.astype('float')<=17.0) | (a.bmi.astype('float')>=41)].copy()
a=a.loc[~((a.height_outlier_jira==1) | (a.weight_outlier_jira))]
if a.shape[0]>0:
    print("BMI OUTLIERS:\n", a.loc[(a.bmi.astype('float') <= 17) | (a.bmi.astype('float') >= 41)])
    a['code']='PINK'
    a['datatype']='REDCap'
    a['reason']='BMI is an outlier.  Please double check height and weight'
    a['issueCode']='AE7001'
    a=a[['subject','redcap_event','study_id', 'site','reason','code','issueCode','event_date','datatype']]


#missings
bmiv=bmiv.loc[bmiv.bmi=='']
if bmiv.shape[0]>0:
    bmiv['code']='PINK'
    bmiv['issueCode']='AE3001'
    bmiv['datatype']='REDCap'
    bmiv['reason']='Missing Height or Weight (or there is another typo preventing BMI calculation)'
    #bmiv=bmiv.loc[(bmiv.age_visit.astype(float).isnull()==True)]
    print("Missing BMI:\n",bmiv.loc[bmiv.bmi==''])
    bmiv=bmiv[['subject','redcap_event','study_id', 'site','reason','code','issueCode','event_date','datatype']]

#check counterbalance distribution - 97 vs 82 (3 to 4) as o 1/27/23
inventoryaabc.loc[inventoryaabc.redcap_event_name.astype('str').str.contains('register')].counterbalance_1st.value_counts()

##############################################################################

#####
#all the flags for JIRA together
QAAP=concat(Q1,Q2,a1,a11,a111,a2,P,C,summv,agemv,ageav,a, bmiv,T,Hot1,Hot2b).drop(columns=['v0_date'])
QAAP['QCdate'] = date.today().strftime("%Y-%m-%d")
QAAP['issue_age']=(pd.to_datetime(QAAP.QCdate) - pd.to_datetime(QAAP.event_date))
QAAP=QAAP[['subject','redcap_event','study_id', 'site','reason','code','issueCode','event_date','issue_age','datatype']].drop_duplicates()
QAAP.sort_values(['site','issue_age'],ascending=False).to_csv('All_Issues_'+date.today().strftime("%d%b%Y")+'.csv',index=False)

###REDUCE by Color code.... need to be able to change these values.
filteredQ=QAAP.loc[((QAAP.code=='PINK') & (QAAP.issue_age.dt.days>7)) | ((QAAP.code=='RED')) | ((QAAP.code=='RED') & (QAAP.issue_age.dt.days.isnull()==True)) |  ((QAAP.code=='ORANGE') & (QAAP.issue_age.dt.days>18)) |  ((QAAP.code=='YELLOW') & (QAAP.issue_age.dt.days>28)) |  ((QAAP.code=='GREEN') & (QAAP.issue_age.dt.days>35)) ]
filteredQ.to_csv('FilteredQC4Jira.csv',index=False)

#NOW DO a Fresh download of everything, drop all issues, and send snapshot.
#Use inventory as  gold standard and make sure visit is complete.

keepvars=['study_id','redcap_event_name','site','subject_id','v0_date','event_date','age','sex','ethnic','racial','legacy_yn','height_ft','height_in','bmi','psuedo_guid']
aabcinvent=getframe(struct=aabcreport,api_url=config['Redcap']['api_url10'])
inventoryaabc=idvisits(aabcinvent,keepsies=keepvars)
inventoryaabc['PIN']=inventoryaabc.subject+"_"+inventoryaabc.redcap_event
subjects=aabcinvent[['study_id','register_visit_complete']]
inventoryaabc = inventoryaabc.loc[~(inventoryaabc.subject_id.str.upper().str.contains('TEST'))].copy()
subjects=subjects.loc[subjects.register_visit_complete =='2'][['study_id']]
subs=list(subjects.study_id)
inventoryaabc=inventoryaabc.loc[inventoryaabc.study_id.isin(subs)].copy()
inventoryaabc.loc[inventoryaabc.event_date=='','event_date']=inventoryaabc.v0_date
inventoryaabc=inventoryaabc.drop(columns=['v0_date'])

#drop those with missing ids
#grab PINs to drop datatypes
# remap redcap_event BEFORE FU

issuesfile='All_Issues_'+date.today().strftime("%d%b%Y")+'.csv'
issues=pd.read_csv(issuesfile)
issues=issues.loc[(issues.subject.isnull()==False)& (issues.datatype !="REDCap")].copy()
issues['PIN']=issues.subject+"_"+issues.redcap_event
issues=issues[['PIN','datatype','subject','redcap_event']].copy()
len(issues.PIN.unique())

inventorysnapshot=pd.merge(inventoryaabc,issues.drop(columns=['PIN']),on=['subject','redcap_event'],how='outer',indicator=True)
inventorysnapshot=inventorysnapshot.loc[inventorysnapshot._merge=='left_only'].drop(columns=['_merge'])

################STOP HERE UNLESS YOU WANT T CREATE SNAPSHOTS ############################
#########################################################################################
#ASA24
BIGGESTTotalsRest,BIGGESTTotals2=PINfirst(BIGGESTTotals,"Totals",issuesfile,inventorysnapshot[['subject','redcap_event']],restrictedATotals);
BIGGESTItemsRest,BIGGESTItems2=PINfirst(BIGGESTItems,"Items",issuesfile,inventorysnapshot[['subject','redcap_event']],restrictedAItems)
BIGGESTRespRest,BIGGESTResp2=PINfirst(BIGGESTResp,"Resp",issuesfile,inventorysnapshot[['subject','redcap_event']],restrictedAResp)
BIGGESTTSRest,BIGGESTTS2=PINfirst(BIGGESTTS,"TS",issuesfile,inventorysnapshot[['subject','redcap_event']],restrictedATS)
BIGGESTTNSRest,BIGGESTTNS2=PINfirst(BIGGESTTNS,"TNS",issuesfile,inventorysnapshot[['subject','redcap_event']],restrictedATNS)
BIGGESTINSRest,BIGGESTINS2=PINfirst(BIGGESTINS,"INS",issuesfile,inventorysnapshot[['subject','redcap_event']],restrictedAINS)

box.upload_file("./tmp/AABC_"+"ASA24-"+ "Totals_" + date.today().strftime("%Y-%m-%d") + '.csv', Asnaps)
box.upload_file("./tmp/AABC_"+"ASA24-"+ "Items_" + date.today().strftime("%Y-%m-%d") + '.csv', Asnaps)
box.upload_file("./tmp/AABC_"+"ASA24-"+ "Resp_" + date.today().strftime("%Y-%m-%d") + '.csv', Asnaps)
box.upload_file("./tmp/AABC_"+"ASA24-"+ "TS_" + date.today().strftime("%Y-%m-%d") + '.csv', Asnaps)
box.upload_file("./tmp/AABC_"+"ASA24-"+ "TNS_" + date.today().strftime("%Y-%m-%d") + '.csv', Asnaps)
box.upload_file("./tmp/AABC_"+"ASA24-"+ "INS_" + date.today().strftime("%Y-%m-%d") + '.csv', Asnaps)

box.upload_file("./tmp/AABC_"+"ASA24-"+ "Totals" +"_Restricted_" + date.today().strftime("%Y-%m-%d") + '.csv', Rsnaps)
box.upload_file("./tmp/AABC_"+"ASA24-"+ "Items" +"_Restricted_" + date.today().strftime("%Y-%m-%d") + '.csv', Rsnaps)
box.upload_file("./tmp/AABC_"+"ASA24-"+ "Resp" +"_Restricted_" + date.today().strftime("%Y-%m-%d") + '.csv', Rsnaps)
box.upload_file("./tmp/AABC_"+"ASA24-"+ "TS" +"_Restricted_" + date.today().strftime("%Y-%m-%d") + '.csv', Rsnaps)
box.upload_file("./tmp/AABC_"+"ASA24-"+ "TNS" +"_Restricted_" + date.today().strftime("%Y-%m-%d") + '.csv', Rsnaps)
box.upload_file("./tmp/AABC_"+"ASA24-"+ "INS" +"_Restricted_" + date.today().strftime("%Y-%m-%d") + '.csv', Rsnaps)

#qintdf2 is already filtered for not unusables - inventory filtered for issues
qintdf2uploadrestricted=pd.merge(inventorysnapshot[['subject','redcap_event']],qintdf2,left_on=['subject','redcap_event'],right_on=['subjectid','redcap_event'],how='inner')
qintdf2uploadrestricted['PIN']=qintdf2uploadrestricted.subject+"_"+qintdf2uploadrestricted.redcap_event
qintdf2upload=qintdf2uploadrestricted.drop(columns=restrictedQ+['Qint'])
qintdf2uploadrestricted.to_csv("AABC_Q-Interactive_Restricted" + date.today().strftime("%Y-%m-%d") + '.csv',index=False)
qintdf2upload.to_csv("AABC_Q-Interactive_" + date.today().strftime("%Y-%m-%d") + '.csv',index=False)
box.upload_file("AABC_Q-Interactive_Restricted" + date.today().strftime("%Y-%m-%d") + '.csv', Rsnaps)
box.upload_file("AABC_Q-Interactive_" + date.today().strftime("%Y-%m-%d") + '.csv', Asnaps)

###########################################################
#REDCAP
aabc=getframe(struct=aabcarms,api_url=config['Redcap']['api_url10'])
#figure out drop/restr vars for redcap. these aren't right
aabcidvisitsrestricted=idvisits(aabc,keepsies=list(aabc.columns))
aabcidvisitsrestricted2=pd.merge(aabcidvisitsrestricted,inventorysnapshot[['subject','redcap_event']],on=['subject','redcap_event'],how='inner')
aabcidvisits=aabcidvisitsrestricted2.drop(columns=rdrop+rrest)
aabcidvisitsrestricted2.to_csv("AABC_REDCap_Restricted" + date.today().strftime("%Y-%m-%d") + '.csv',index=False)
aabcidvisits.to_csv("AABC_REDCap_" + date.today().strftime("%Y-%m-%d") + '.csv',index=False)
box.upload_file("AABC_REDCap_Restricted" + date.today().strftime("%Y-%m-%d") + '.csv', Rsnaps)
box.upload_file("AABC_REDCap_" + date.today().strftime("%Y-%m-%d") + '.csv', Asnaps)

###########################################################
#TOOLBOX
#RAW
rf2full=rf2full.drop_duplicates()
len(rf2full.PIN.unique())
rf2full.shape
rf2fullrestricted=pd.merge(rf2full,inventorysnapshot[['PIN']],on=['PIN'],how='inner')
len(rf2fullrestricted.PIN.unique())

rf2fullrestricted.to_csv("AABC_NIH-Toolbox-Raw_Restricted_" + date.today().strftime("%Y-%m-%d") + '.csv',index=False)
rf2fullrestricted.drop(columns=rraw).to_csv("AABC_NIH-Toolbox-Raw_" + date.today().strftime("%Y-%m-%d") + '.csv',index=False)

box.upload_file("AABC_NIH-Toolbox-Raw_Restricted_" + date.today().strftime("%Y-%m-%d") + '.csv', Rsnaps)
box.upload_file("AABC_NIH-Toolbox-Raw_" + date.today().strftime("%Y-%m-%d") + '.csv', Asnaps)

#SCORES
dffull=dffull.drop_duplicates()
len(dffull.PIN.unique())
dffull.shape
dffullrestricted=pd.merge(dffull,inventorysnapshot[['PIN']],on=['PIN'],how='inner')
len(dffullrestricted.PIN.unique())

dffullrestricted.to_csv("AABC_NIH-Toolbox-Scores_Restricted_" + date.today().strftime("%Y-%m-%d") + '.csv',index=False)
dffullrestricted.drop(columns=rscore).to_csv("AABC_NIH-Toolbox-Scores_" + date.today().strftime("%Y-%m-%d") + '.csv',index=False)

box.upload_file("AABC_NIH-Toolbox-Scores_Restricted_" + date.today().strftime("%Y-%m-%d") + '.csv', Rsnaps)
box.upload_file("AABC_NIH-Toolbox-Scores_" + date.today().strftime("%Y-%m-%d") + '.csv', Asnaps)

########################################################################
#Cobra
CobraRestricted=pd.merge(Cobra,inventorysnapshot[['PIN','subject','redcap_event']],on="PIN",how='inner')
#list of restricted variables: rcobras
RCOBRAS = [y.upper() for y in rcobras]

CobraRestricted.to_csv("AABC_Actigraphy-Summary_Restricted_" + date.today().strftime("%Y-%m-%d") + '.csv',index=False)
CobraRestricted.drop(columns=RCOBRAS).to_csv("AABC_Actigraphy-Summary_" + date.today().strftime("%Y-%m-%d") + '.csv',index=False)

box.upload_file("AABC_Actigraphy-Summary_Restricted_" + date.today().strftime("%Y-%m-%d") + '.csv', Rsnaps)
box.upload_file("AABC_Actigraphy-Summary_" + date.today().strftime("%Y-%m-%d") + '.csv', Asnaps)

# Already: Redcap, TLBX, ASA24, Q-interactive, Actigraphy,
# don't upload Psychopy.
# NEXT: Inventory and Hotflash
#c.loc[b.PIN != '']
TINS=BIGGESTINS2[['PIN']].drop_duplicates(subset='PIN').copy()
TINS['ASA_TINS']='YES'

Totals=BIGGESTTotals2[['PIN']].drop_duplicates(subset='PIN').copy()
Totals['ASA_Totals']='YES'

TNS=BIGGESTTNS2[['PIN']].drop_duplicates(subset='PIN').copy()
TNS['ASA_TNS']='YES'

Items=BIGGESTItems2[['PIN']].drop_duplicates(subset='PIN').copy()
Items['ASA_Items']='YES'

Resp=BIGGESTResp2[['PIN']].drop_duplicates(subset='PIN').copy()
Resp['ASA_Resp']='YES'

TS=BIGGESTTS2[['PIN']].drop_duplicates(subset='PIN').copy()
TS['ASA_TS']='YES'

Q=qintdf2upload[['PIN']].drop_duplicates(subset='PIN').copy()
Q['Qint']='YES'

TLBX=dffullrestricted[['PIN']].drop_duplicates(subset='PIN').copy()
TLBX['TLBX']='YES'

Act=CobraRestricted[['PIN']].copy()
Act['Actigraphy_Cobra']='YES'

InventA=inventorysnapshot.merge(Act.merge(TLBX.merge(Q,on='PIN',how='left'),on='PIN',how='left'),on='PIN',how='left')
InventB=InventA.merge(TS.merge(Resp,on='PIN',how='left'),on='PIN',how='left')
InventoryRestricted=InventB.merge(TNS.merge(TINS.merge(Items.merge(Totals,on='PIN',how='left'),on='PIN',how='left'),on='PIN',how='left'),on='PIN',how='left')
InventoryRestricted.to_csv("AABC_Inventory_Restricted_" + date.today().strftime("%Y-%m-%d") + '.csv',index=False)
box.upload_file("AABC_Inventory_Restricted_" + date.today().strftime("%Y-%m-%d") + '.csv', Rsnaps)
InventoryRestricted.drop(columns=['event_date']).to_csv("AABC_Inventory_" + date.today().strftime("%Y-%m-%d") + '.csv',index=False)
box.upload_file("AABC_Inventory_" + date.today().strftime("%Y-%m-%d") + '.csv', Asnaps)



