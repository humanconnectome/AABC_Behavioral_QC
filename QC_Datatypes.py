# %%
# TODO: deepen actigraphy and hotflash QC
# TODO: freeze at Nov 1
# to do: drop scan_collectyn=0

#the following subjects have visit notes about missing data but are reporting to have collected actigraphy.
#please correct visit summary:
#4129-94	v2_inperson_arm_8
#4129-101	v2_inperson_arm_8
#4129-134	v2_inperson_arm_6
#4130-8	v3_inperson_arm_2
PRCfolder='340425049927'
# %%
import pandas as pd
from ccf.box import LifespanBox
import re
import collections
from functions import *
from config import *
from datetime import date
import requests
import matplotlib.pyplot as plt
from itables import show

# %% [markdown]
# ## Main check

# %%
DNR = ["HCA7787304_V1", "HCA6276071_V1", "HCA6229365_V1", "HCA9191078_V1", "HCA6863086_V1"]
#These guys accidentally recruited as V2
v2oops=['HCA6686191','HCA7296183']


# %%
## get configuration files
outp="/Users/petralenzini/work/Behavioral/AABC/AABC_Behavioral_QC/AABC_Behavioral_QC/tmp/"
raiyanfile=outp+"cobracommentsJuly2025.csv"

config = LoadSettings()
secret=pd.read_csv(config['config_files']['secrets'])
intradb=pd.read_csv(config['config_files']['PCP'])
box = LifespanBox(cache=outp)
#aabc_processing=config['aabc_processing']


# %%
#fixed data
#gwasfamfile=statsdir+"HCA_imputed_geno0.02_final.fam"
#gwasHCA=list(pd.read_csv(gwasfamfile,sep='\t',header=None)[0].unique()) #949
#fixed data APOE and metabolites
#metabolites1 (change this to a config location)
#metabolites2 (change this to a config location)

# %%
aabcdictionary='AABC_REDCap_DataDictionary_2023-05-15.csv'                 # aabc REDCap data dictionary...necessary for automating variables at appropriate events - put in tmpdir
#AABCdict=pd.read_csv(outp+aabcdictionary,low_memory=False)
E=pd.read_csv(box.downloadFile(config['encyclopedia']),low_memory=False,encoding='ISO-8859-1')
SSAGAvars=list(E.loc[E['Form / Instrument'].str.upper().str.contains('SSAGA'),'Variable / Field Name'])

# %%
## get the HCA inventory for ID checking with AABC
pathp=box.downloadFile(config['hcainventory'])

# %%
#get current version variable mask from BOX (for excluding variables just prior to sending snapshots to PreRelease for investigator access)
# add redundant race/ethnicity varas as well as hidden variables to restricted and remove from encyclopedia

# %%
#Asnaps=config['aabc_pre']
#Rsnaps=config['aabc_pre_restricted']
#encyc=box.downloadFile(config['encyclopedia'])
#E=pd.read_csv(encyc)

#SWITCH.  USE ENCYCLOPEDIA, NOT THIS SRPEADSHEET
#a=box.downloadFile(config['variablemask'])
#rdrop=getlist(a,'AABC-ARMS')
#rrest=getlist(a,'AABC-ARMS')
#rraw=getlist(a,'TLBX-RAW')
#rscore=getlist(a,'TLBX-SCORES')
#restrictedQ=getlist(a,'Q')
#restrictedATotals=getlist(a,'ASA24-Totals')
#restrictedAResp=getlist(a,'ASA24-Resp')
#restrictedATS=getlist(a,'ASA24-TS')
#restrictedAINS=getlist(a,'ASA24-INS')
#restrictedATNS=getlist(a,'ASA24-TNS')
#restrictedAItems=getlist(a,'ASA24-Items')
#rcobras=getlist(a,'COBRAS')

# %%
#get ids
ids=pd.read_csv(pathp)
hcaids=ids.subject.drop_duplicates()
#for later use in getting the last visit for each participant in HCA so that you can later make sure that person is starting subsequent visit and not accidentally enrolled in the wrong arm
hca_lastvisits=ids[['subject','redcap_event']].loc[ids.redcap_event.isin(['V1','V2'])].sort_values('redcap_event').drop_duplicates(subset='subject',keep='last')

# %%
#########################################################################################
#PHASE 0 TEST IDS AND ARMS
aabcarms = redjson(tok=secret.loc[secret.source=='aabcarms','api_key'].reset_index().drop(columns='index').api_key[0])
hcpa = redjson(tok=secret.loc[secret.source=='hcpa','api_key'].reset_index().drop(columns='index').api_key[0])
aabcreport = redreport(tok=secret.loc[secret.source=='aabcarms','api_key'].reset_index().drop(columns='index').api_key[0],reportid='51031')

#download the inventory report from AABC for comparison
aabcinvent=getframe(struct=aabcreport,api_url=config['Redcap']['api_url10'])
failscreen=list(aabcinvent.loc[(aabcinvent.passedscreen.isin(['0'])) & (aabcinvent.subject_id!='')]['subject_id'])





# %%
#find subjectts who have completed a visit and are not DNR - save for later
reds=aabcinvent.loc[aabcinvent.register_visit_complete =='2'][['study_id']]
inperson=list(reds.study_id.unique())
reds2=aabcinvent.loc[(aabcinvent.study_id.isin(inperson)) & (~(aabcinvent.subject_id =='')) & (~(aabcinvent.subject_id.isin(DNR)))]
inpersonHCAid=list(reds2.subject_id.unique())

# %%
#trying to set study_id from config file, but have been sloppy...there are instances where the actual subject_id has been coded below
study_id=config['Redcap']['datasources']['aabcarms']['redcapidvar']

#slim selects just the registration event (V0) because thats where the ids and legacy information is kept.
slim=aabcinvent[['study_id','redcap_event_name',study_id,'legacy_yn','site','v0_date']].loc[(aabcinvent.redcap_event_name.str.contains('register'))]

#compare aabc ids against hcaids and whether legacy information is properly accounted for (e.g. legacy variable flags and actual event in which participannt has been enrolled.
fortest=pd.merge(hcaids,slim,left_on='subject',right_on=study_id,how="outer",indicator=True)
#fortest._merge.value_counts()
legacyarms=['register_arm_1','register_arm_2','register_arm_3','register_arm_4','register_arm_5','register_arm_6','register_arm_7','register_arm_8']

# %%
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



# %%
# Check if subject fail the screen but came in for visit
pass_failed=aabcinvent.loc[(aabcinvent.passedscreen =='2') & (aabcinvent['subject_id'].astype(str).str.strip() != '')][['subject_id', 'study_id', 'redcap_event_name', 'site','v0_date']]
qlist4=pd.DataFrame()
if not pass_failed.empty:
    pass_failed['reason']='subject did not pass screen but came in for imaging - need confirmation'
    pass_failed['code']='RED'
    pass_failed['issueCode'] = 'AE1001'
    pass_failed['datatype']='REDCap'
    qlist4 = pass_failed[['subject_id', 'study_id', 'redcap_event_name', 'site','reason','code','v0_date','datatype']]
    for s4 in list(pass_failed[study_id].unique()):
        print('CODE RED :',s4,': subject did not pass screen but came in for imaging - need confirmation')

# %%
#if legacy v1 and enrolled as if v3 or v4 or legacy v2 and enrolled v4
#get last visit
hca_lastvisits["next_visit"]=''
#idvisits rolls out the subject ids to all visits. get subects current visit for comparison with last visit
aabcidvisits=idvisits(aabcinvent,keepsies=['study_id','redcap_event_name','site','subject_id','v0_date','event_date'])
sortaabc=aabcidvisits.sort_values(['study_id','redcap_event_name'])
sortaabcv=sortaabc.loc[~(sortaabc.redcap_event_name.str.contains('register'))]
sortaabcv.drop_duplicates(subset=['study_id'],keep='first')
print("OOOPSs:",sortaabcv.loc[sortaabcv.subject.isin(v2oops)])
#add 1 to last visit from HCA
#also set up for checking to make sure not initiating same visit
hca_lastvisits.next_visit=hca_lastvisits.redcap_event.str.replace('V','').astype('int') +1
hca_lastvisits["next_visit2"]="V"+hca_lastvisits.next_visit.astype(str)
hca_lastvisits2=hca_lastvisits.drop(columns=['redcap_event','next_visit'])

# %%
#check that current visit in AABC is the last visit in HCA + 1
check=pd.merge(hca_lastvisits2,sortaabcv,left_on=['subject','next_visit2'],right_on=['subject','redcap_event'],how='outer',indicator=True)
check=check.loc[check._merge !='left_only']
wrongvisit=check.loc[check._merge=='right_only']
wrongvisit=wrongvisit.loc[~(wrongvisit.redcap_event.isin(['AP']))]#,'v1_inperson_arm_10','v1_inperson_arm_12']))]
wrongvisit=wrongvisit.loc[wrongvisit.next_visit2.isnull()==False]

# %%
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

# %%
#check to make sure they aren't initiating the same visit.
check2=pd.merge(hca_lastvisits[['subject','redcap_event']],sortaabcv,left_on=['subject','redcap_event'],right_on=['subject','redcap_event'],how='inner')
check2=check2.loc[~(check2.subject.isin(v2oops))]
qlist32=pd.DataFrame()
if not check2.empty:
    check2['reason']='Subject found in AABC REDCap Database initiating the wrong visit sequence (e.g. V3 insteady of V2'
    check2['code']='RED'
    check2['issueCode'] = 'AE1001'
    check2['datatype']='REDCap'
    qlist32 = check2[['subject', 'study_id', 'redcap_event_name', 'site','reason','code','v0_date','event_date','datatype']]
    qlist32=qlist32.rename(columns={'subject':'subject_id'})
    for s3 in list(check2['subject'].unique()):
        if s3 !='':
            print('CODE RED (if HCA6911778 ignore) :',s3,': Subject found in AABC REDCap Database initiating the wrong visit sequence (e.g. V3 insteady of V2')
            qlist32=qlist32.loc[~(qlist32.subject_id=='HCA6911778')].copy()

# %%
#test subjects that need to be deleted
tests=aabcinvent.loc[(aabcinvent[study_id].str.upper().str.contains('TEST')) | (aabcinvent[study_id].str.upper().str.contains('PRAC')) | (aabcinvent[study_id].str.upper().str.contains('DEMO'))][['study_id',study_id,'redcap_event_name']]
qlist5=pd.DataFrame()
if not tests.empty:
    tests['reason']='HOUSEKEEPING : Please delete test subject.  Use test database when practicing'
    tests['code']='HOUSEKEEPING'
    tests['datatype']='REDCap'
    tests['issueCode'] = 'AE6001'
    qlist5 = tests[['subject_id', 'study_id', 'redcap_event_name', 'site','reason','code','v0_date','event_date','datatype']]
    for s5 in list(tests[study_id].unique()):
        print('HOUSEKEEPING : Please delete test subject:', s5)

# %%
# Get the REDCap AABC inventory (which may or may not agree with the reality of data found):
# and remove the test subjects, since we have already flagged them
#also remove the DNR and screen failures

inventoryaabc=idvisits(aabcinvent,keepsies=list(aabcinvent.columns))
inventoryaabc['PIN']=inventoryaabc.subject+"_"+inventoryaabc.redcap_event
print('test subjects:',inventoryaabc.loc[(inventoryaabc.subject_id.str.upper().str.contains('TEST'))])
inventoryaabc = inventoryaabc.loc[~(inventoryaabc.subject_id.str.upper().str.contains('TEST'))].copy()
inventoryaabc=inventoryaabc.loc[~(inventoryaabc.PIN.isin(DNR))]


# %%
# Croms checks
#Flag anyone with problematic croms_income data
inc1=inventoryaabc.loc[(inventoryaabc.croms_income.str.contains('999')) & (~(inventoryaabc.croms_income=='-9999'))].croms_income.value_counts()
inc2=inventoryaabc.loc[(inventoryaabc.croms_income.str.contains('\,')) |(inventoryaabc.croms_income.str.contains('\$')) | (inventoryaabc.croms_income.str.upper().str.contains('Y')) | (inventoryaabc.croms_income.str.upper().str.contains('D')) | (inventoryaabc.croms_income.str.upper().str.contains('R'))]
a=inventoryaabc.loc[(~(inventoryaabc.croms_income=='')) & (~(inventoryaabc.croms_income.str.contains('\-'))) & (inventoryaabc.croms_income_confirmed!="1")]
#a=inventoryaabc.loc[(~(inventoryaabc.croms_income=='')) & (~(inventoryaabc.croms_income.str.contains('\-')))]
a.croms_income.value_counts()
formatx=a.loc[a.croms_income.str.contains(',')].copy()
a.loc[a.croms_income.str.contains(r'\+')].croms_income
inc3a=a.loc[~((a.croms_income.str.contains(',')) | (a.croms_income.str.contains(r'\+')))]


inc3 = inc3a.loc[(inc3a.croms_income.astype(int) < 1000) | (inc3a.croms_income.astype(int) > 500000)]
inc=pd.concat([inc1,inc2,inc3,formatx])
print(inc.shape)
inc=inc.loc[inc.croms_income.isnull()==False]
print(inc.shape)
qlist6=pd.DataFrame()
if not inc.empty:
    inc['reason']='Invalid Format or Suspicious Croms Income reported'
    inc['code']='RED'
    inc['issueCode'] = 'AE1001'
    inc['datatype']='REDCap'
    qlist6 = inc[['subject', 'study_id', 'redcap_event_name', 'site','reason','code','v0_date','event_date','datatype','issueCode']]
    qlist6 = qlist6.rename(columns={'subject':'subject_id'})
    for s6 in list(inc['subject'].unique()):
        if s6 !='':
            print('CODE RED :',s6,': Invalid or Highly Suspicious Croms Income reported')


fipscheck=inventoryaabc.loc[inventoryaabc.redcap_event_name.str.contains('register')].copy()
fipscols=[a for a in inventoryaabc.columns if 'fips' in a and 'collectyn' not in a]
wonkyfips=pd.DataFrame()
# Step 2: Remove leading zeros (assuming columns are string or convertible to string)
for col in fipscols:
    print(col)
    fipscheck[col] = fipscheck[col].astype(str).str.lstrip('0')
    fipscheck['fips_len'] = fipscheck[col].astype(str).str.len()
    # Replace empty strings with NaN (as stripping zeros can lead to empty strings)
    # Step 2: Find the most common length
    common_len = fipscheck['fips_len'].mode()[0]
    # Step 3: Find rows where length is unexpected
    if 'county' not in col:
        df_outliers = fipscheck[(fipscheck['fips_len'] != common_len) & (fipscheck['fips_len'] != 0)]
        print(df_outliers[fipscols])#['subject_id','redcap_event_name']+
    if 'county' in col:
        df_outliers = fipscheck[(fipscheck['fips_len'] > 3) & (fipscheck['fips_len'] != 0)]
    fipscheck[col] = fipscheck[col].replace('', pd.NA)
    wonkyfips=pd.concat([wonkyfips,df_outliers],axis=0)
wonkyfips=wonkyfips.drop_duplicates()

# Step 3: Identify rows where any of the fipscols is null/missing
missing_fips_mask = fipscheck[fipscols].isna().any(axis=1)

# Step 4: Check where fips_collectyn is not 0
collectyn_mask = fipscheck['fips_collectyn'] != '1'
# Combine both conditions
result = fipscheck[missing_fips_mask & collectyn_mask]
resultfips=pd.concat([result,wonkyfips],axis=0)
resultfips=resultfips.loc[resultfips.redcap_event_name.str.contains('register')]

#if not resultfips.empty:
#    resultfips['reason']='Geocoder info for FIPS is missing or contains unexpected data'
#    resultfips['code']='RED'
#    resultfips['issueCode'] = 'AE1001'
#    resultfips['datatype']='REDCap'
#    qlist7 = resultfips[['subject', 'study_id', 'redcap_event_name', 'site','reason','code','v0_date','event_date','datatype','issueCode']]
#    qlist7 = qlist7.rename(columns={'subject':'subject_id'})
#    for s7 in list(resultfips['subject'].unique()):
#        if s7 !='':
#            print('CODE RED :',s7,': Geocoder info for FIPS is missing or contains unexpected data')#



# %%
###concatenate Phase 0 flags for REDCap key variables
Q0=pd.DataFrame(columns=['subject_id', 'study_id', 'redcap_event_name', 'site','reason','issue_code','code','v0_date','event_date'])
try:
    Q1 = concat(*[Q0,qlist1,qlist2,qlist3,qlist4,qlist32,qlist5,qlist6,qlist7])
except:
    Q1=pd.DataFrame(columns=['subject_id', 'study_id', 'redcap_event_name', 'site','reason','issue_code','code','v0_date','event_date'])

Q1=Q1.rename(columns={'subject_id':'subject'})
Q1.event_date=Q1.v0_date
Q1['redcap_event']='V0'

# %%
#########################################################################################
# NEXT: NOW Test that all dataypes expected are present
# Q data grabber turned into a cron job via Qscratch and /Users/petralenzini/cron/runcron_AABC_QC.sh

#QC checks
qintreport = redreport(tok=secret.loc[secret.source=='qint','api_key'].reset_index().drop(columns='index').api_key[0],reportid='51037')

#aabcreport = redreport(tok=secret.loc[secret.source=='aabcarms','api_key'].reset_index().drop(columns='index').api_key[0],reportid='51031')

qintdf2=getframe(struct=qintreport,api_url=config['Redcap']['api_url10'])
#invq=qintdf2[['id', 'site', 'subjectid','visit','q_unusable']].copy()
qintdf2['redcap_event']="V"+qintdf2.visit
qintdf2['Qint']='YES'
qintdf2=qintdf2.loc[~(qintdf2.q_unusable=='1')].copy()
dups2=qintdf2.loc[qintdf2.duplicated(subset=['subjectid','visit'])]

# %%
q0=pd.DataFrame()
if dups2.shape[0]>0:
    print("Duplicate Q-interactive records")
    print(dups2[['subjectid','visit']])
    q0=dups2.copy()
    q0['datatype']='RAVLT'
    q0['reason']='Duplicate Q-interactive records'
    q0['code']='ORANGE'
    q0['issueCode']='AE5001'

# %%
inventoryaabc2=pd.merge(inventoryaabc.drop(columns=['visit']),qintdf2.drop(columns=['site','redcap_data_access_group']),left_on=['subject','redcap_event'],right_on=['subjectid','redcap_event'],how='outer',indicator=True)
q1=pd.DataFrame()
if inventoryaabc2.loc[inventoryaabc2._merge=='right_only'].shape[0] > 0 :
    print("The following ID(s)/Visit(s) are not found in the main AABC-ARMS Redcap.  Please investigate")
    print(inventoryaabc2.loc[inventoryaabc2._merge=='right_only'][['subjectid','redcap_event','site']])
    q1=inventoryaabc2.loc[inventoryaabc2._merge=='right_only'][['subjectid','redcap_event','site']].rename(columns={'subjectid':'subject'})
    q1['reason']='Subject with Q-int data but ID(s)/Visit(s) are not found in the main AABC-ARMS Redcap.  Please look for typo or naming convention problem and forward correction instructions and ticket to Petra'
    q1['code']='ORANGE'
    q1['issueCode']='AE1001'
    q1['datatype']='RAVLT'

# %%
inventoryaabc3=inventoryaabc2.loc[inventoryaabc2._merge!='right_only'].drop(columns=['_merge'])

#pull in the unusables again
unuse=getframe(struct=qintreport,api_url=config['Redcap']['api_url10'])[['subjectid','visit','q_unusable']]
unuse['redcap_event']="V"+unuse.visit
unuse=unuse.rename(columns={'subjectid':'subject'}).copy()
inventoryaabc3.drop(columns=['q_unusable','visit']).merge(unuse,on=['subject','redcap_event'],how='left')
#inventoryaabc3 = inventoryaabc3.merge(unuse,on=['subject','redcap_event'],how='left')

qcols=['subject','redcap_event','fileid','filename','sha1','created','assessment','visit','form','q_unusable','unusable_specify','common_complete','ravlt_two','ravlt_pea_ravlt_sd_tc','ravlt_delay_scaled','ravlt_delay_completion','ravlt_discontinue','ravlt_reverse','ravlt_pea_ravlt_sd_trial_i_tc','ravlt_pea_ravlt_sd_trial_ii_tc','ravlt_pea_ravlt_sd_trial_iii_tc','ravlt_pea_ravlt_sd_trial_iv_tc','ravlt_pea_ravlt_sd_trial_v_tc','ravlt_pea_ravlt_sd_listb_tc','ravlt_pea_ravlt_sd_trial_vi_tc','ravlt_recall_correct_trial1','ravlt_recall_correct_trial2','ravlt_recall_correct_trial3','ravlt_recall_correct_trial4','ravlt_delay_recall_correct','ravlt_delay_recall_intrusion','ravlt_delay_total_intrusion','ravlt_delay_total_repetitions']
inventoryaabc3[qcols].loc[inventoryaabc3.redcap_event.str.contains('V')].to_csv(os.path.join(outp,"Qinteractive_processing_2025-09-11.csv"),index=False)

# %%

inventoryaabc3=inventoryaabc3.loc[~(inventoryaabc3.q_unusable_y=='1')].copy()

# %%
#inventoryaabc3

# %%
missingQ=inventoryaabc3.loc[(inventoryaabc3.redcap_event_name.str.contains('v')) & (~(inventoryaabc3.Qint=='YES')) & (~(inventoryaabc3.ravlt_collectyn=='0')) ][['subject_id','study_id','subject','redcap_event','site','event_date','ravlt_collectyn']]
missingQ=missingQ.loc[~(missingQ.event_date=='')]
missingQ=missingQ.loc[~(missingQ.subject=='')]

q2=pd.DataFrame()
if missingQ.shape[0]>0:
    print("Q-interactive cannot be found for")
    print(missingQ)
    q2=missingQ.copy()
    q2['reason']='MISSING, MISNAMED, or MALFORMED Q-interactive data for this subject/visit'
    q2['code']='ORANGE'
    q2['issueCode']='AE4001'
    q2['datatype']='RAVLT'

# %%
# HCA8065270, HCA8192378  visit:H
# HCA9099999    HCA909999_?

# %%
Q0=pd.DataFrame(columns=['subject_id', 'study_id', 'redcap_event_name', 'site','reason','issueCode','code','v0_date','event_date','datatype'])
Q2=pd.concat([Q0,q0,q1,q2],axis=0)
Q2['subject_id']=Q2.subject

# %%
#Q2

# %%
# TO DO: exclude the subjects marked as un-usable.
# eg.https://redcap.wustl.edu/redcap/redcap_v14.0.16/DataEntry/index.php?pid=12674&page=common&event_id=47060&id=86&instance=1

# %% [markdown]
# ## TOOLBOX QC (hide for now)

# %%
#aabc_processing = box.list_of_files([str(config['aabc_processing'])])
#process_files=pd.DataFrame.from_dict(aabc_processing, orient='index')


# %%
#tlbx_raw=process_files.loc[(process_files.filename.str.contains('RAW'))].copy()
#tlbx_raw['datestamp']=tlbx_raw.filename.str.split('_',expand=True)[3]
#tlbx_raw['datatype']=tlbx_raw.filename.str.split('_',expand=True)[2]
#tlbx_raw=tlbx_raw.loc[(tlbx_raw.datestamp.str.contains('.csv')==True) & (tlbx_raw.datestamp.str.contains('-'))].copy()
#tlbx_raw.datestamp=tlbx_raw.datestamp.str.replace('.csv','')
#tlbx_raw.datestamp=pd.to_datetime(tlbx_raw.datestamp)
#tlbx_raw=tlbx_raw.sort_values('datestamp',ascending=False)
#tlbx_raw=tlbx_raw.drop_duplicates(subset='datatype',keep='first').copy()

# %%
# NOW FOR TOOLBOX. ############################################################################
# # 1. grab partial files from intraDB (on chpc...see tlbxscan.py) - note that you'll need to run dos2unix on the results.
# # 2. QC (after incorporating patches)
# # 3. generate tickets and send to JIra if don't already exist
# # 4. send tickets that arent identical to ones already in Jira
# # 5. CONCATENATE legit data (A scores file and a Raw file, no test subjects or identical duplicates -- no 'Narrow' or 'Registration' datasets)
# # 6. create and send snapshot of patched data to BOX after dropping restricted variables

# %%
#rf2=pd.read_csv(box.downloadFile(tlbx_raw['fileid'][0]),low_memory=False,encoding='ISO-8859-1')

#FIRST read it in.
rf2full=pd.read_csv(outp+"temp_TLBX_RAW.csv",low_memory=False)
rf2full=rf2full.loc[~(rf2full.PIN.str.upper().str.contains('HCA1234567_V1'))]
rf2full=rf2full.loc[~(rf2full.PIN.str.upper().str.contains('TEST'))]
rf2full=rf2full.loc[~(rf2full.PIN.str.upper().str.contains('HCP TESR'))]
rf2full=rf2full.loc[~(rf2full.PIN.str.upper().str.contains('HCA1111111_CR'))]
rf2full=rf2full.loc[~(rf2full.PIN.str.upper()=='ABC123')]
rf2full=rf2full.loc[~(rf2full.PIN.str.upper()=='HCA9926201')] #invalid format typo fixed
rf2full=rf2full.loc[~(rf2full.PIN.str.upper().str.contains('PRACTICE'))]
rf2full=rf2full.loc[~(rf2full.PIN.str.contains('No line'))]
rf2full=rf2full.loc[~(rf2full.PIN=="HCA9202360_V3")].copy() #this should be excluded but not sure
rf2full=rf2full.loc[~(rf2full.PIN.str.contains('No line'))]
rf2full=rf2full.loc[~(rf2full.PIN=="7677802_V3")].copy()
rf2full=rf2full.loc[~(rf2full.PIN=="")]

#NOW THE SCORED DATA
dffull=pd.read_csv(outp+"temp_TLBX_Scores.csv")
dffull=dffull.loc[~(dffull.PIN.str.upper().str.contains('HCA1234567_V1'))]
dffull=dffull.loc[~(dffull.PIN.str.upper().str.contains('TEST'))]
dffull=dffull.loc[~(dffull.PIN.str.upper().str.contains('HCP TESR'))]
dffull=dffull.loc[~(dffull.PIN.str.upper().str.contains('HCA1111111_CR'))]
dffull=dffull.loc[~(dffull.PIN.str.upper()=='ABC123')]
dffull=dffull.loc[~(dffull.PIN.str.upper()=='HCA9926201')]
dffull=dffull.loc[~(dffull.PIN.str.upper().str.contains('PRACTICE'))]
dffull=dffull.loc[~(dffull.PIN.str.contains('No line'))]
dffull=dffull.loc[~(dffull.PIN=="HCA9202360_V3")].copy()
dffull=dffull.loc[~(dffull.PIN=="7677802_V3")].copy()
dffull=dffull.loc[~(dffull.PIN=="")]


#Registration data
#lastly clean the registration data:
dfreg=pd.read_csv(outp+"temp_TLBX_Registration.csv",low_memory=False)
dfreg=dfreg.loc[~(dfreg.PIN.str.upper().str.contains('HCA1234567_V1'))]
dfreg=dfreg.loc[~(dfreg.PIN.str.upper().str.contains('TEST'))]
dfreg=dfreg.loc[~(dfreg.PIN.str.upper().str.contains('HCP TESR'))]
dfreg=dfreg.loc[~(dfreg.PIN.str.upper().str.contains('HCA1111111_CR'))]
dfreg=dfreg.loc[~(dfreg.PIN.str.upper()=='ABC123')]
dfreg=dfreg.loc[~(dfreg.PIN.str.upper()=='HCA9926201')]
dfreg=dfreg.loc[~(dfreg.PIN.str.upper().str.contains('PRACTICE'))]
dfreg=dfreg.loc[~(dfreg.PIN.str.contains('No line'))]
dfreg=dfreg.loc[~(dfreg.PIN=="HCA9202360_V3")].copy()
dfreg=dfreg.loc[~(dfreg.PIN=="7677802_V3")].copy()
dfreg=dfreg.loc[~(dfreg.PIN=="")]

#REMOVE any rows corresponding to Practice instruments or instructions
dffull=dffull.loc[~(dffull.Inst.str.upper().str.contains('PRACTICE'))]
dffull=dffull.loc[~(dffull.Inst.str.upper().str.contains('INSTRUCTIONS'))]
rf2full=rf2full.loc[~(rf2full.Inst.str.upper().str.contains('PRACTICE'))]
rf2full=rf2full.loc[~(rf2full.Inst.str.upper().str.contains('INSTRUCTIONS'))]

inventoryaabc['PIN']=inventoryaabc.subject + "_" + inventoryaabc.redcap_event

fixtypos=inventoryaabc.loc[~(inventoryaabc.nih_toolbox_upload_typo=="")]
fixtypos.loc[~(fixtypos.nih_toolbox_upload_typo.str.upper().str.contains('NIGHTMARE'))]
#NOW SWAP THE TYPOS FOR NON-NIGHTMARES
fixtyposNOT=fixtypos.loc[~(fixtypos.nih_toolbox_upload_typo.str.upper().str.contains('NIGHTMARE'))].copy()
fixes=dict(zip(fixtypos.nih_toolbox_upload_typo, fixtypos.PIN))

rf2full.PIN=rf2full.PIN.replace(fixes)
dffull.PIN=dffull.PIN.replace(fixes)
dfreg.PIN=dfreg.PIN.replace(fixes)

#peek for remaining wierdness
print("Unique PINs in Data:",len(rf2full.PIN.unique()))
print("Unique PINs in Scores:",len(dffull.PIN.unique()))
print("Unique PINs in Registration:",len(dfreg.PIN.unique()))
print("Unique PINs in all files combined:",len(set(list(rf2full.PIN.unique())+list(dffull.PIN.unique())+list(dfreg.PIN.unique()))))


print("Data shape:",rf2full.shape)
print("Scores shape",dffull.shape)
print("Registration shape",dfreg.shape)

#delete duplicates (round1)
dffull=dffull.drop_duplicates(subset=['PIN','Assessment Name','Inst','RawScore','Theta','TScore','SE','ItmCnt','DateFinished','Uncorrected Standard Score','Age-Corrected Standard Score','National Percentile (age adjusted)','Fully-Corrected T-score','Uncorrected Standard Scores Dominant','Age-Corrected Standard Scores Dominant','National Percentile (age adjusted) Dominant','Fully-Corrected T-scores Dominant','Uncorrected Standard Scores Non-Dominant','Age-Corrected Standard Scores Non-Dominant','National Percentile (age adjusted) Non-Dominant','Fully-Corrected T-scores Non-Dominant','Dominant Score','Non-Dominant Score','Raw Score Right Ear','Threshold Right Ear','Raw Score Left Ear','Threshold Left Ear','Static Visual Acuity logMAR','Static Visual Acuity Snellen']).copy()
rf2full=rf2full.drop_duplicates(subset=['PIN','DeviceID','Assessment Name','InstOrdr','InstSctn','ItmOrdr','Inst','Locale','ItemID','Response','Score','Theta','TScore','SE','DataType','Position','ResponseTime','DateCreated'])
dfreg=dfreg.drop_duplicates()

print("Data shape:",rf2full.shape)
print("Scores shape",dffull.shape)
print("Registration shape",dfreg.shape)

#export temp copy:
rf2full.to_csv("temp2_TLBX_RAW.csv",index=False)
dffull.to_csv("temp2_TLBX_Scores.csv",index=False)
dfreg.to_csv("temp2_TLBX_Registration.csv",index=False)

#this one was a nightmare that was reintroduced...making hard coded fix:
rf2full=rf2full.loc[~((rf2full.PIN=="HCA8596099_V3") & (rf2full['App Version'].isin(['1.25.6068','1.27.7237',''])))]#['App Version'].value_counts(dropna=False)
dffull=dffull.loc[~((dffull.PIN=="HCA8596099_V3") & (dffull['App Version'].isin(['1.25.6068','1.27.7237',''])))]#['App Version'].value_counts(dropna=False)
dfreg=dfreg.loc[~((dfreg.PIN=="HCA8596099_V3") & (dfreg['Name'].str.contains("NOT")))]#['App Version'].value_counts(dropna=False)

#there was also a second Words in Noise run for the this typo, so need to remove Assessment 1.
dffull=dffull.loc[~((dffull.PIN=="HCA8596099_V3") & (dffull.InstrumentRCReasonOther.str.contains("Interrupt")))]


print("Data shape:",rf2full.shape)
print("Scores shape",dffull.shape)
print("Registration shape",dfreg.shape)

#nightmares have to be replaced by going into intradb, deleting the old record and uploading a new, fixed version
#did we miss any?
nightmares=inventoryaabc.loc[inventoryaabc.nih_toolbox_upload_typo.str.upper().str.contains('NIGHTMARE')]

#f=['HCA8596099_V2','HCA7802172_V2','HCA8923391_V3','HCA9372789_V3']
#there shouldn't be any of f in the list of nightmares unless bad data were re-introduced
#other nightmares that need to be addressed
for i in list(nightmares.PIN):
    print(rf2full.loc[rf2full.PIN==i].drop_duplicates(subset='PIN'))
for i in list(nightmares.PIN):
    print(dffull.loc[dffull.PIN==i].drop_duplicates(subset='PIN'))
for i in list(nightmares.PIN):
    print(dfreg.loc[dfreg.PIN==i].drop_duplicates(subset='PIN'))


# %% Preston's old code
##tlbx_scores=process_files.loc[(process_files.filename.str.contains('Scores'))].copy()
##tlbx_scores['datestamp']=tlbx_scores.filename.str.split('_',expand=True)[3]
##tlbx_scores['datatype']=tlbx_scores.filename.str.split('_',expand=True)[2]
##tlbx_scores=tlbx_scores.loc[(tlbx_scores.datestamp.str.contains('.csv')==True) & (tlbx_scores.datestamp.str.contains('-'))].copy()
##tlbx_scores.datestamp=tlbx_scores.datestamp.str.replace('.csv','')
##tlbx_scores.datestamp=pd.to_datetime(tlbx_scores.datestamp)
##tlbx_scores=tlbx_scores.sort_values('datestamp',ascending=False)
##tlbx_scores=tlbx_scores.drop_duplicates(subset='datatype',keep='first').copy()

# %%
##dffull=pd.read_csv(box.downloadFile(tlbx_scores['fileid'][0]),low_memory=False,encoding='ISO-8859-1')
#duplicate calculation for Cog Composites on different app.  not sure what happened.
#dffull=dffull.loc[~((dffull['App Version']=='1.27.7219') & (dffull.PIN=='HCA6597293_V2'))]
#dffull=dffull.loc[~((dffull['App Version']=='1.27.7219') & (dffull.PIN=='HCA8307773_V2'))]
#dffull=dffull.loc[~((dffull['App Version']=='1.27.7219') & (dffull.PIN=='HCA8904185_V3'))]

#now find duplicate instruments

#print(dffull.shape)
#dffull=dffull.loc[~((dffull['App Version']=='1.27.7219') & (dffull.PIN=='HCA7651278_V3'))]
#print(dffull.shape)
###dffull=dffull.loc[~((dffull['App Version']=='1.28.7262') & (dffull.PIN=='HCA8244977_V3'))]
#print(dffull.shape)
#dffull=dffull.loc[~((dffull['App Version']=='1.28.7265') & (dffull.PIN=='HCA9189899_V1'))]
#print(dffull.shape)


# %%
#HCA6131550_V1 had two bad assessments for "NIH Toolbox Words-In-Noise Test Age 6+ v2.1"
#dffull=dffull.loc[~((dffull.PIN == "HCA6131550_V1") & (dffull.Inst == "NIH Toolbox Words-In-Noise Test Age 6+ v2.1") & (dffull['Assessment Name'].isin(['Assessment 1','Assessment 2'])))]
#rf2full=rf2full.loc[~((rf2full.PIN == "HCA6131550_V1") & (rf2full.Inst == "NIH Toolbox Words-In-Noise Test Age 6+ v2.1") & (rf2full['Assessment Name'].isin(['Assessment 1','Assessment 2'])))]

# %%
#merge with patch fixes (i.e. delete duplicate specified in visit summary)
# This is a single fix... need to generalized to all instruments and their corresponding dupvars:
# -->HCA8596099_V3 has 2 assessments for Words in Noise - add patch note"


#now get rid of known dups
iset=inventoryaabc
dffull=filterdupass('NIH Toolbox Words-In-Noise Test Age 6+ v2.1','tlbxwin_dups_v2',iset,dffull)
print(dffull.shape)
dffull=filterdupass('NIH Toolbox 2-Minute Walk Endurance Test Age 3+ v2.0','walkendur_dups',iset,dffull)
print(dffull.shape)
dffull=filterdupass('NIH Toolbox 4-Meter Walk Gait Speed Test Age 7+ v2.0','tlbx4walk_dups',iset,dffull)
print(dffull.shape)
dffull=filterdupass('NIH Toolbox List Sorting Working Memory Test Age 7+ v2.1','tlbxlist_dups',iset,dffull)
print(dffull.shape)
dffull=filterdupass('NIH Toolbox Grip Strength Test Age 3+ v2.0','tlbxgrip_dups',iset,dffull)
print(dffull.shape)
dffull=filterdupass('NIH Toolbox Pattern Comparison Processing Speed Test Age 7+ v2.1','tlbxpcps_dups',iset,dffull)
print(dffull.shape)
dffull=filterdupass('NIH Toolbox Picture Sequence Memory Test Age 8+ Form A v2.1','tlbxpsmt_dups',iset,dffull)
print(dffull.shape)
dffull=filterdupass('NIH Toolbox Visual Acuity Test Age 8+ v2.0','tlbxvis_dups',iset,dffull)
print(dffull.shape)

#extra wrinkles from rescoring
dffull=dffull.loc[~((dffull.Inst=="NIH Toolbox Visual Acuity Test Age 8+ v2.0") & (dffull.InstrumentRCReasonOther.str.contains('rescored')))]
print(dffull.shape)

rf2full=filterdupass('NIH Toolbox Words-In-Noise Test Age 6+ v2.1','tlbxwin_dups_v2',iset,rf2full)
print(rf2full.shape)
rf2full=filterdupass('NIH Toolbox 2-Minute Walk Endurance Test Age 3+ v2.0','walkendur_dups',iset,rf2full)
print(rf2full.shape)
rf2full=filterdupass('NIH Toolbox 4-Meter Walk Gait Speed Test Age 7+ v2.0','tlbx4walk_dups',iset,rf2full)
print(rf2full.shape)
rf2full=filterdupass('NIH Toolbox List Sorting Working Memory Test Age 7+ v2.1','tlbxlist_dups',iset,rf2full)
print(rf2full.shape)
rf2full=filterdupass('NIH Toolbox Grip Strength Test Age 3+ v2.0','tlbxgrip_dups',iset,rf2full)
print(rf2full.shape)
rf2full=filterdupass('NIH Toolbox Pattern Comparison Processing Speed Test Age 7+ v2.1','tlbxpcps_dups',iset,rf2full)
print(rf2full.shape)
rf2full=filterdupass('NIH Toolbox Picture Sequence Memory Test Age 8+ Form A v2.1','tlbxpsmt_dups',iset,rf2full)
print(rf2full.shape)
rf2full=filterdupass("NIH Toolbox Visual Acuity Test Age 8+ v2.0",'tlbxvis_dups',iset,rf2full)
print(rf2full.shape)

#special dups from ipad glitches:
dffull=dffull.loc[~((dffull.PIN=="HCA9082881_V1") & (dffull.DeviceID=='NIH-TOOLBOX-BA95418B-FD08-471D-8763-12E67AF61E82') & (dffull.Inst.isin(['NIH Toolbox Picture Vocabulary Test Age 3+ v2.1','NIH Toolbox Picture Sequence Memory Test Age 8+ Form A v2.1','NIH Toolbox Pattern Comparison Processing Speed Test Age 7+ v2.1','NIH Toolbox Oral Reading Recognition Test Age 3+ v2.1','NIH Toolbox List Sorting Working Memory Test Age 7+ v2.1','NIH Toolbox Dimensional Change Card Sort Test Age 12+ v2.1','NIH Toolbox Flanker Inhibitory Control and Attention Test Age 12+ v2.1'])))]
rf2full=rf2full.loc[~((rf2full.PIN=="HCA9082881_V1") & (rf2full.DeviceID=='NIH-TOOLBOX-BA95418B-FD08-471D-8763-12E67AF61E82') & (rf2full.Inst.isin(['NIH Toolbox Picture Vocabulary Test Age 3+ v2.1','NIH Toolbox Picture Sequence Memory Test Age 8+ Form A v2.1','NIH Toolbox Pattern Comparison Processing Speed Test Age 7+ v2.1','NIH Toolbox Oral Reading Recognition Test Age 3+ v2.1','NIH Toolbox List Sorting Working Memory Test Age 7+ v2.1','NIH Toolbox Dimensional Change Card Sort Test Age 12+ v2.1','NIH Toolbox Flanker Inhibitory Control and Attention Test Age 12+ v2.1'])))]
dfreg=dfreg.loc[~((dfreg.PIN=="HCA9082881_V1") & (dfreg.DeviceID=='NIH-TOOLBOX-BA95418B-FD08-471D-8763-12E67AF61E82'))]
dffull=dffull.loc[~((dffull.PIN=='HCA9189899_V1') & ( dffull["Age-Corrected Standard Score"].isnull()==True) & (dffull.Inst.isin(['Cognition Fluid Composite v1.1','NIH Toolbox Dimensional Change Card Sort Test Age 12+ v2.1','NIH Toolbox Flanker Inhibitory Control and Attention Test Age 12+ v2.1','NIH Toolbox List Sorting Working Memory Test Age 7+ v2.1','NIH Toolbox Pattern Comparison Processing Speed Test Age 7+ v2.1','NIH Toolbox Picture Sequence Memory Test Age 8+ Form A v2.1'])))]
dffull=dffull.loc[~(
    (dffull.PIN == "HCA8244977_V3") & (
        (dffull.InstrumentRCReasonOther == "rescored") |
        (
            dffull.Inst.str.contains("Cognition") &
            dffull["National Percentile (age adjusted)"].isin([67, 57, 46])
        )
    )
)
]
dffull=dffull.loc[~(
    (dffull.PIN == "HCA7651278_V3") & (
        (
            dffull.Inst.isin(["Cognition Total Composite Score v1.1","Cognition Early Childhood Composite v1.1"]) &
            dffull["National Percentile (age adjusted)"].isin([6,14])
        )
        |
        (
            dffull.Inst.isin(["Cognition Fluid Composite v1.1","Cognition Crystallized Composite v1.1"]) &
            dffull["Age-Corrected Standard Score"].isin([88,72])
        )
        |
        (dffull.InstrumentRCReasonOther.str.contains("rescored"))
    )
)
]
#remove List sorting from the first iPad results
ipad1="NIH-TOOLBOX-BA95418B-FD08-471D-8763-12E67AF61E82"
ipad2="NIH-TOOLBOX-93023744-8FCF-4066-B422-D835EF8A5261"
badinst1=['NIH Toolbox List Sorting Working Memory Test Age 7+ v2.1']
badinst2=["NIH Toolbox Picture Vocabulary Test Age 3+ v2.1","NIH Toolbox Flanker Inhibitory Control and Attention Test Age 12+ v2.1"]
dffull=dffull.loc[~((dffull.PIN=="HCA7024655_V3") & (dffull.DeviceID==ipad1) & (dffull.Inst.isin(badinst1)))]
dffull=dffull.loc[~((dffull.PIN=="HCA7024655_V3") & (dffull.DeviceID==ipad2) & (dffull.Inst.isin(badinst2) ))]
rf2full=rf2full.loc[~((rf2full.PIN=="HCA7024655_V3") & (rf2full.DeviceID==ipad1) & (rf2full.Inst.isin(badinst1)))]
rf2full=rf2full.loc[~((rf2full.PIN=="HCA7024655_V3") & (rf2full.DeviceID==ipad2) & (rf2full.Inst.isin(badinst2) ))]

#a NIGHTMARE.  There were 2 assessment 3 HCA9247483_V2 for badinst1;
#dffull=\
dffull=dffull.loc[~((dffull.PIN=="HCA9247483_V2") & (dffull.Inst.isin(badinst1)) & (dffull.RawScore.isnull()==True) ) ]
#no raw data asspcoated with scores
#another oddity only exists in scores (app interruptions):
dffull=dffull.loc[~((dffull.PIN=="HCA7841182_V1") & (dffull.Inst=="NIH Toolbox Words-In-Noise Test Age 6+ v2.1") & (dffull['Assessment Name'].isin(['Assessment 1','Assessment 2'])))].copy()
#rf2full.loc[~((rf2full.PIN=="HCA7841182_V1") & (rf2full.Inst=="NIH Toolbox Words-In-Noise Test Age 6+ v2.1"))]# & (rf2full['Assessment Name'].isin(['Assessment 1','Assessment 2'])))]

print("Data shape:",rf2full.shape)
print("Scores shape",dffull.shape)
print("Registration shape",dfreg.shape)

#delete duplicates (round2)
dffull=dffull.drop_duplicates(subset=['PIN','Assessment Name','Inst','RawScore','Theta','TScore','SE','ItmCnt','DateFinished','Uncorrected Standard Score','Age-Corrected Standard Score','National Percentile (age adjusted)','Fully-Corrected T-score','Uncorrected Standard Scores Dominant','Age-Corrected Standard Scores Dominant','National Percentile (age adjusted) Dominant','Fully-Corrected T-scores Dominant','Uncorrected Standard Scores Non-Dominant','Age-Corrected Standard Scores Non-Dominant','National Percentile (age adjusted) Non-Dominant','Fully-Corrected T-scores Non-Dominant','Dominant Score','Non-Dominant Score','Raw Score Right Ear','Threshold Right Ear','Raw Score Left Ear','Threshold Left Ear','Static Visual Acuity logMAR','Static Visual Acuity Snellen']).copy()
rf2full=rf2full.drop_duplicates(subset=['PIN','DeviceID','Assessment Name','InstOrdr','InstSctn','ItmOrdr','Inst','Locale','ItemID','Response','Score','Theta','TScore','SE','DataType','Position','ResponseTime','DateCreated'])
dfreg=dfreg.drop_duplicates()

print("Data shape:",rf2full.shape)
print("Scores shape",dffull.shape)
print("Registration shape",dfreg.shape)

dffull.replace(np.nan,'',  inplace=True)
rf2full.replace(np.nan,'', inplace=True)
dfreg.replace(np.nan,'',  inplace=True)

dffull = dffull.applymap(lambda x: x.strip() if isinstance(x, str) else x)
rf2full = rf2full.applymap(lambda x: x.strip() if isinstance(x, str) else x)
dfreg = dfreg.applymap(lambda x: x.strip() if isinstance(x, str) else x)

dffull=dffull.drop_duplicates(subset=['PIN','Assessment Name','Inst','RawScore','Theta','TScore','SE','ItmCnt','DateFinished','Uncorrected Standard Score','Age-Corrected Standard Score','National Percentile (age adjusted)','Fully-Corrected T-score','Uncorrected Standard Scores Dominant','Age-Corrected Standard Scores Dominant','National Percentile (age adjusted) Dominant','Fully-Corrected T-scores Dominant','Uncorrected Standard Scores Non-Dominant','Age-Corrected Standard Scores Non-Dominant','National Percentile (age adjusted) Non-Dominant','Fully-Corrected T-scores Non-Dominant','Dominant Score','Non-Dominant Score','Raw Score Right Ear','Threshold Right Ear','Raw Score Left Ear','Threshold Left Ear','Static Visual Acuity logMAR','Static Visual Acuity Snellen']).copy()
rf2full=rf2full.drop_duplicates(subset=['PIN','DeviceID','Assessment Name','InstOrdr','InstSctn','ItmOrdr','Inst','Locale','ItemID','Response','Score','Theta','TScore','SE','DataType','Position','ResponseTime','DateCreated'])
dfreg=dfreg.drop_duplicates()

print("Data shape:",rf2full.shape)
print("Scores shape",dffull.shape)
print("Registration shape",dfreg.shape)

#export temp copy:
rf2full.to_csv("Upload_Processing_TLBX_RAW.csv",index=False)
dffull.to_csv("Upload_Processing_TLBX_Scores.csv",index=False)
dfreg.to_csv("Upload_Processing_TLBX_Registration.csv",index=False)


# %%
#find any non-identical duplicated Assessments still in data after patch
#.drop(columns=['App Version'])
#.drop(columns=['App Version'])

#dffulltmp=dffull[['PIN','Assessment Name','Inst','RawScore','Theta','TScore','SE','ItmCnt','DateFinished','Uncorrected Standard Score','Age-Corrected Standard Score','National Percentile (age adjusted)','Fully-Corrected T-score','Uncorrected Standard Scores Dominant','Age-Corrected Standard Scores Dominant','National Percentile (age adjusted) Dominant','Fully-Corrected T-scores Dominant','Uncorrected Standard Scores Non-Dominant','Age-Corrected Standard Scores Non-Dominant','National Percentile (age adjusted) Non-Dominant','Fully-Corrected T-scores Non-Dominant','Dominant Score','Non-Dominant Score','Raw Score Right Ear','Threshold Right Ear','Raw Score Left Ear','Threshold Left Ear','Static Visual Acuity logMAR','Static Visual Acuity Snellen']].drop_duplicates()

#FIND REMAINING DUPS.
dupass=dffull.loc[dffull.duplicated(subset=['PIN','Inst'],keep=False)][['PIN','Assessment Name','Inst']]

#TURN THIS INTO A TICKET
duptix=dupass.drop_duplicates(subset='PIN').copy()

#duptix=dupass.drop_duplicates().copy()
duptix=dupass.drop_duplicates().copy()
#this is not actually a duplicate in the temp files...
duptix=duptix.loc[~(duptix.PIN.isin(["HCA9901386_V2","HCA6743682_V1","HCA7802172_V3","HCA8249078_V3","HCA8364482_V3","HCA9729299_V2"]))].copy()

if duptix.shape[0]>0:
    duptix['code']='ORANGE'
    duptix['issueCode'] = 'AE5001'
    duptix['datatype'] = 'TLBX'
    duptix['reason'] = "Non-Identical Duplicate Found for "+duptix.Inst
    i3=inventoryaabc[['subject', 'study_id', 'redcap_event_name', 'redcap_event','event_date']].copy()
    i3["PIN"]=i3.subject+'_'+i3.redcap_event
    duptix=pd.merge(duptix,i3,on='PIN',how='left')

duptix.to_csv(outp+"Dups.csv",index=False)

#drop duplicates from registration - ipad is irrelevant
dfreg=dfreg.drop_duplicates(subset=[i for i in dfreg.columns if i != "DeviceID"]).copy()

# %%
#QC check:
#Either scored or raw is missing in format expected:
formats=pd.merge(dffull.PIN.drop_duplicates(),rf2full.PIN.drop_duplicates(),how='outer',on='PIN',indicator=True)[['PIN','_merge']]
issues=formats.loc[~(formats._merge=='both')]
t1=pd.DataFrame()
if issues.shape[0]>0:
    t1=issues.copy()
    t1['code']='ORANGE'
    t1['issueCode']='AE5001'
    t1['datatype']='TLBX'
    t1['reason']="Please re-export.  Raw or Scored data not found (make sure you don't export Narrow format)"
    print("Raw or Scored data not found (make sure you didn't export Narrow format)")
    print(issues[['PIN']])

# %%
#find cases where PIN was reused (e.g. PIN is the same but date more than 3 weeks different
#extend this section by pivoting and subtracting dates, then searching for diffs>60 days.
# still working on this...not sure how to turn into ticket...maybe just check with Angela
dffull['Date']=dffull.DateFinished.str.split(' ', expand=True)[0]
catchdups=dffull.loc[~(dffull.Date.isnull()==True)]
c=catchdups.drop_duplicates(subset=['PIN','Date'])[['PIN','Date']]
first=c.loc[c.duplicated(subset='PIN',keep='first')]
last=c.loc[c.duplicated(subset='PIN',keep='last')]
#c.loc[c.duplicated(subset='PIN',keep=True)]
cmerge=pd.merge(first,last,on='PIN',how='inner')
cmerge['datediff']=(pd.to_datetime(cmerge['Date_x'])-pd.to_datetime(cmerge['Date_y'])).dt.days
cmerge.loc[cmerge['datediff']>15]
cmerge['reason']="Parts of Toolbox for "+cmerge.PIN+ "were completed > 4 weeks apart"
cmerge['redcap_event']=cmerge.PIN.str.split("_",expand=True)[1]
cmerge['subject']=cmerge.PIN.str.split("_",expand=True)[0]
cmerge['redcap_event']=cmerge.PIN.str.split("_",expand=True)[1]
#now merge with inventory
cmerge=pd.merge(inventoryaabc[['site','subject','redcap_event']],cmerge,on=['subject','redcap_event'],how='inner')
cflag=cmerge.loc[cmerge.datediff>22]
tc=pd.DataFrame()
if cflag.shape[0]>0:
    tc=cflag.copy()
    tc['code']='ORANGE'
    tc['issueCode']='AE5001'
    tc['datatype']='TLBX'
    tc['reason']="Time elapsed between TLBX start and TLBX finished is greater than 22 days"
    print("Time elapsed between TLBX start and TLBX finished is greater than 22 days")
    print(tc[['PIN']])

print("ignore HCA8889115_V3 (due to splitting tlbx collection between 3T and 7T visits) and HCA9854303_V4  (ipad glitch)")

#these are checked and addressed

# %%
#add subject and visit
df2=dffull.drop_duplicates(subset='PIN').copy()
df2['redcap_event']=df2.PIN.str.split("_",expand=True)[1]
df2['subject']=df2.PIN.str.split("_",expand=True)[0]
df2['redcap_event']=df2.PIN.str.split("_",expand=True)[1]
df2['TLBX']='YES'

#now merge with inventory
inventoryaabc4=pd.merge(inventoryaabc.drop(columns=['PIN']),df2[['subject','redcap_event','TLBX','PIN']],on=['subject','redcap_event'],how='outer',indicator=True)#.rename(columns={'site':'siteT'})

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

# %%
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
    print(inventoryaabc4.loc[inventoryaabc4._merge=='right_only'][['PIN','subject','redcap_event','site']])
t2b=pd.DataFrame()
if df2.loc[df2.PIN.str.len()>13].shape[0] >0 :
    t2b=df2.loc[df2.PIN.str.len()>13].copy()
    t2b['reason']='TOOLBOX PINs are not found in the main AABC-ARMS Redcap.  Typo?'
    t2b['code']='ORANGE'
    t2b['issueCode']='AE1001'
    t2b['datatype']='TLBX'
    print("The following TOOLBOX PINs are not found in the main AABC-ARMS Redcap.  Please investigate")
    print(df2.loc[df2.PIN.str.len()>13][['PIN','subject','redcap_event','site']])

# %%
#t2b=t2b.loc[~t2b.PIN.str.contains('HCA855')]
inventoryaabc4=inventoryaabc4.loc[inventoryaabc4._merge!='right_only'].drop(columns=['_merge'])

# Look for missing IDs
missingT=inventoryaabc4.loc[(inventoryaabc4.redcap_event_name.str.contains('v')) & (~(inventoryaabc4.TLBX=='YES'))]
missingT=missingT.loc[~(missingT.event_date=='')]
missingT=missingT.loc[~(missingT.nih_toolbox_collectyn=='0')]
missingT=missingT.loc[~(missingT.subject=='')]
t3=pd.DataFrame()
if missingT.shape[0]>0:
    t3=missingT.copy()
    t3['reason']='Missing TLBX data'
    t3['code']='ORANGE'
    t3['issueCode']='AE2001'
    t3['datatype']='TLBX'
    print("TLBX cannot be found for")
    print(missingT[['subject','redcap_event','site','event_date','nih_toolbox_collectyn']])

# %%
#Time elapsed is sus:
#363  HCA8889115_V3
#425  HCA9854303_V4

#T=pd.concat([duptix,t1,t2b,t3])[['subject','study_id','redcap_event_name','redcap_event', 'event_date','PIN', 'reason', 'code','issueCode','datatype']]
T=pd.concat([duptix,tc,t1,t2,t3,t2b,tlong])[['subject','study_id','redcap_event_name','redcap_event', 'event_date','PIN', 'reason', 'code','issueCode','datatype']]
#merge with site num from inventory
T=pd.merge(T,inventoryaabc4[['subject','redcap_event','site']],on=['subject','redcap_event'],how='left')
T.to_csv(outp+'ToolboxTix.csv',index=False)

# %% [markdown]
# ## ASA 24 QC

# %%
######################################################################


### NOW For ASA 24 ######################################################################
# ORDER
# 1. scan for data (here just looking for existende)
# 2. QC ids (after incorporating patches and translating ASAID into AABC id)
# 3. generate tickets and send to JIra if don't already exist
#  4. send tickets that arent identical to ones already in Jira
# # # 5. just dump all legit data to BOX (transform to be defined later) after patching, dropping restricted variables, and merging in subject and redcap_event
# # # 6. create and send snapshot of patched data to BOX after dropping restricted variables


# %%
#totals=process_files.loc[(process_files.filename.str.contains('Totals'))].copy()
#totals['datestamp']=totals.filename.str.split('_',expand=True)[2]
#totals['datatype']=totals.filename.str.split('_',expand=True)[1]
#totals=totals.loc[(totals.datestamp.str.contains('.csv')==True) & (totals.datestamp.str.contains('-'))].copy()
#totals.datestamp=totals.datestamp.str.replace('.csv','')
#totals.datestamp=pd.to_datetime(totals.datestamp)
#totals=totals.sort_values('datestamp',ascending=False)
#totals=totals.drop_duplicates(subset='datatype',keep='first').copy()

# %%
#BIGGESTTotals=pd.read_csv(box.downloadFile(totals['fileid'][0]),low_memory=False,encoding='ISO-8859-1')
types = ['Totals', 'INS', 'TNS', 'Resp', 'Items', 'TTS']
sites = ['MGH', 'UCLA', 'UMN', 'WU']

dfs = {}  # dictionary to hold your results
for j in types:
    temp_dfs = []
    for i in sites:
        fname = os.path.join(outp, "ASA", f"{i}_temp_{j}.csv")
        df_temp = pd.read_csv(fname, low_memory=False)
        temp_dfs.append(df_temp)
    dfs[j] = pd.concat(temp_dfs, axis=0, ignore_index=True)
    fout = os.path.join(outp, "ASA", f"temp_{j}.csv")
    dfs[j].to_csv(fout,index=False)
    box.upload_file(fout,PRCfolder)

# %%


BIGGESTTotals=pd.read_csv(outp+"ASA/temp_Totals.csv",low_memory=False)

BIGGESTTotals=BIGGESTTotals.loc[~(BIGGESTTotals.PIN.isnull()==True)]

# %%
#just read in latest stuff from cron-job -- is in ./tmp directory
#BIGGESTTotals=pd.read_csv(outp+'temp_Totals.csv')
#BIGGESTTotals=BIGGESTTotals.loc[~(BIGGESTTotals.PIN.isnull()==True)]
#BIGGESTItems=pd.read_csv(outp+'temp_Items.csv')
#BIGGESTItems=BIGGESTItems.loc[~(BIGGESTItems.PIN.isnull()==True)]
#BIGGESTResp=pd.read_csv(outp+'temp_Resp.csv')
#BIGGESTResp=BIGGESTResp.loc[~(BIGGESTResp.PIN.isnull()==True)]
#BIGGESTTS=pd.read_csv(outp+'temp_TTS.csv')
#BIGGESTTS=BIGGESTTS.loc[~(BIGGESTTS.PIN.isnull()==True)]
#BIGGESTTNS=pd.read_csv(outp+'temp_TNS.csv')
#BIGGESTTNS=BIGGESTTNS.loc[~(BIGGESTTNS.PIN.isnull()==True)]
#BIGGESTINS=pd.read_csv(outp+'temp_INS.csv')
#BIGGESTINS=BIGGESTINS.loc[~(BIGGESTINS.PIN.isnull()==True)]

# %%
AD=BIGGESTTotals[['PIN','UserName']].rename(columns={'PIN':'PIN_perBox'}).copy()
AD['asa24id']=AD.UserName#=pd.DataFrame(anydata,columns=['asa24id'])
AD=AD.drop_duplicates()
AD['ASA24']='YES'

# %%
#missings
inventoryaabc5=pd.merge(inventoryaabc,AD,on='asa24id',how='outer',indicator=True)
missingAD=inventoryaabc5.loc[(inventoryaabc5._merge != 'right_only') & (inventoryaabc5.redcap_event_name.str.contains('v')) & (~(inventoryaabc5.ASA24=='YES'))]
missingAD=missingAD.loc[~(missingAD.asa24yn=='0')]
a1=pd.DataFrame()
if missingAD.shape[0]>0:
    print("ASA24 cannot be found for")
    print(missingAD[['subject','redcap_event','site','event_date','asa24yn','asa24id']])
    a1=missingAD.copy()
    a1['reason']='Unable to locate ASA24 id in Redcap or ASA24 data in Box for this subject visit'
    a1['code']='GREEN'
    a1['datatype']='ASA24'
    a1['issueCode']='AE2001'
    a1['subject_id']=a1['subject']
a1=a1[['subject_id','subject', 'study_id', 'redcap_event','redcap_event_name', 'site','reason','code','issueCode','v0_date','event_date','datatype']]
#a1 is concatenated later with other q2 codes
#typo PIN PINbox is different that PINRedcap
missmatch=inventoryaabc5.loc[(inventoryaabc5.PIN != inventoryaabc5.PIN_perBox) & (inventoryaabc5.asa24id != "") & (inventoryaabc5.ASA24 =="YES")]

# %%
#these ones are bad and associated with multiple ids
misstype1=missmatch.loc[(missmatch.PIN != missmatch.PIN_perBox) & (~(missmatch.PIN.isnull()==True)) & (~(missmatch.PIN_perBox.isnull()==True))]
a11=pd.DataFrame()
if misstype1.shape[0]>0:
    print("ASA24 id matches to different study ids in Box vs Redcap")
    print(misstype1[['subject','redcap_event','site','event_date','asa24yn','asa24id','PIN','PIN_perBox']])
    a11=misstype1.copy()
    a11['reason']='ASA24 id associated with multiple study ids in Box vs Redcap.  Check for typos in subject or visit, or for plain old missingness'
    a11['code']='GREEN'
    a11['datatype']='ASA24'
    a11['issueCode']='AE2001'
    a11['subject_id']=a11['subject']
    a11=a11[['subject_id','subject', 'study_id', 'redcap_event','redcap_event_name', 'site','reason','code','issueCode','v0_date','event_date','datatype']]

# %%
misstype2=missmatch.loc[~((missmatch.PIN != missmatch.PIN_perBox) & (~(missmatch.PIN.isnull()==True)) & (~(missmatch.PIN_perBox.isnull()==True)))]

# %%
# please check that data are placed under the correct id in REDCap and Box.
misstype3=misstype2.loc[misstype2.PIN_perBox.isnull()==True]
a13=pd.DataFrame()
if misstype3.shape[0]>0:
    print('please check that data are linked to the correct record in REDCap and Box via the ASA24 id')
    print(misstype3[['subject','redcap_event','site','event_date','asa24yn','asa24id','PIN','PIN_perBox']])
    a13=misstype3.copy()
    a13['reason']='please check that data are linked to the correct record in REDCap and Box via the ASA24 id'
    a13['code']='GREEN'
    a13['datatype']='ASA24'
    a13['issueCode']='AE2001'
    a13['subject_id']=a13['subject']
    a13=a13[['subject_id','subject', 'study_id', 'redcap_event','redcap_event_name', 'site','reason','code','issueCode','v0_date','event_date','datatype']]


# %%
#these ones have data in box but can't be linked to redcap (typo?)
misstype4=misstype2.loc[misstype2.PIN_perBox.isnull()==False]
a14=pd.DataFrame()
if misstype4.shape[0]>0:
    print('data in box but cant be linked to redcap (typo?)')
    print(misstype4[['subject','redcap_event','site','event_date','asa24yn','asa24id','PIN','PIN_perBox']])
    a14=misstype4.copy()
    a14['reason']="Data exist in box but can't be linked to redcap. Please make sure there aren't typos and that the ASAid is recorded in the visit summary"
    a14['code']='GREEN'
    a14['datatype']='ASA24'
    a14['issueCode']='AE2001'
    a14['subject_id']=a14['subject']
    a14=a14[['subject_id','subject', 'study_id', 'redcap_event','redcap_event_name', 'site','reason','code','issueCode','v0_date','event_date','datatype']]


# %%
#
##typo same PIN but different asa24id
#missmatch2=pd.merge(inventoryaabc,AD,how='inner',left_on='PIN',right_on='PIN_perBox')
#mm=missmatch2.loc[missmatch2.UserName!=missmatch2.asa24id_y]
#a111=pd.DataFrame()
#if mm.shape[0]>0:
#    print("Subject associated with 2 ASA24 ids in Box vs Redcap")
#    print(mm[['subject','redcap_event','site','event_date','asa24yn','asa24id','PIN_perBox']])
#    a111=mm.copy()
#    a111['reason']='Subject associated with 2 ASA24 ids in Box vs Redcap"'
#    a111['code']='GREEN'
#    a111['datatype']='ASA24'
#    a111['issueCode']='AE2001'
#    a111['subject_id']=a1['subject']
#    a111=a111[['subject_id','subject', 'study_id', 'redcap_event','redcap_event_name', 'site','reason','code','issueCode','v0_date','event_date','datatype']]

# %% [markdown]
# ## ACTIGRAPHY

# %%
#################################################################################
#ACTIGRAPHY
### for now, this is basically the same protocol as for ASA24
#move scan to cron

# %%
#actigraphy=process_files.loc[(process_files.filename.str.contains('actigraphy'))].copy()
#actigraphy['datestamp']=actigraphy.filename.str.split('_',expand=True)[2]
#actigraphy['datatype']=actigraphy.filename.str.split('_',expand=True)[1]
#actigraphy=actigraphy.loc[(actigraphy.datestamp.str.contains('.csv')==True) & (actigraphy.datestamp.str.contains('-'))].copy()
#actigraphy.datestamp=actigraphy.datestamp.str.replace('.csv','')
#actigraphy.datestamp=pd.to_datetime(actigraphy.datestamp)
#actigraphy=actigraphy.sort_values('datestamp',ascending=False)
#actigraphy=actigraphy.drop_duplicates(subset='datatype',keep='first').copy()

# %%
#actigraphy

# %%
#actdatat=pd.read_csv(box.downloadFile(actigraphy['fileid'][0]),low_memory=False,encoding='ISO-8859-1')
actdatat=pd.read_csv(outp+'temp_actigraphy.csv',low_memory=False,encoding='ISO-8859-1')#[['PIN','watch','site','D','other']]
actdatat=actdatat.drop_duplicates()
actdatat=actdatat.loc[~(actdatat.PIN.isnull()==True)]
def collapse_watch(watch_series):
    unique_vals = watch_series.unique()
    if len(unique_vals) > 1:
        return 'BOTH'
    else:
        return unique_vals[0]

collapsed = actdatat.groupby('PIN').agg({
    'watch': collapse_watch,
    'site': 'first',
    'D': 'first'
}).reset_index()



#actdata=list(actdatat.PIN.unique())#.to_list()
# %%
#actdata=actdatat.PIN.to_list()

# %%
#Duplicates?
#if [item for item, count in collections.Counter(actdata).items() if count > 1] != '':
#    print('Duplicated Actigraphy Record Found:',[item for item, count in collections.Counter(actdata).items() if count > 1])

# %%
#ActD=pd.DataFrame(actdata,columns=['PIN']).drop_duplicates(subset='PIN')
#ActD['Actigraphy']='YES'
ActD=collapsed.copy()
ActD['Actigraphy']='YES'

#actigraphy_upload_typoyn - don't want to fix these on the fly - use as reference with COBRAs

fixtypos=inventoryaabc.loc[inventoryaabc.actigraphy_upload_typo!=''][['subject','redcap_event','actigraphy_upload_typo']]
fixtypos['PIN']=fixtypos.subject+'_'+fixtypos.redcap_event
fixtypos=fixtypos.loc[fixtypos.actigraphy_upload_typo!='NIGHTMARE']
#temporarily turn off fixing to generate report for COBRA
#fixes=dict(zip(fixtypos.actigraphy_upload_typo, fixtypos.PIN))
#ActD.PIN=ActD.PIN.replace(fixes)

# %%
inventoryaabc2=pd.merge(inventoryaabc,ActD.drop(columns=['site']),on='PIN',how='left')

cobramerge=inventoryaabc2.loc[(inventoryaabc2.redcap_event_name.str.contains('v'))][['subject','redcap_event','site','actigraphy_collectyn','actigraphy_partial_1___1','actigraphy_upload_typo','camntech_yn','watch','D','Actigraphy']]
cobramerge['Site']=cobramerge.site.replace({'1':'MGH','2':'UCLA','3':'UMN','4':'WashU'})
cobramerge=cobramerge.drop(columns=['site'])

cobramerge['FailedScreen']=''
cobramerge.loc[cobramerge.subject.isin(failscreen),'FailedScreen']='YES'
cobramerge.to_csv(outp+'test.csv')

#get current Freeze expectation and merge in for COBRA inventory
cobramergeFreeze=pd.read_csv("/Users/petralenzini/work/Behavioral/AABC/AABC_Behavioral_QC/AABC_Behavioral_QC/Union_HCA-AABC_Data-Freeze-2_id-events_2025-07-09.csv").drop(columns='Site')

cobramergeFreezeforCobra=pd.merge(cobramerge,cobramergeFreeze,on=['subject','redcap_event'],how='outer',indicator=True)
cobramergeFreezeforCobra._merge.value_counts()
cobramergeFreezeforCobraAABC=cobramergeFreezeforCobra.loc[~(cobramergeFreezeforCobra.study=='HCA')].drop(columns=['race','age_visit','MRS_Freeze1','MRS_Collected','study','M/F','ethnic_group','age_bin'])
cobramergeFreezeforCobraAABC._merge.value_counts()
cobramergeFreezeforCobraAABC['AnyActigraphyDataCollected']=cobramergeFreezeforCobraAABC.actigraphy_collectyn.replace({'0':'No Data','1':'Device and Diary','2':'Partial'})
#cobramergeFreezeforCobraAABC['DeviceDataCollected']=cobramergeFreezeforCobraAABC.actigraphy_partial_1___1.replace({'0':'','1':'YES','2':'Partial'})

cobramergeFreezeforCobraAABC.loc[cobramergeFreezeforCobraAABC._merge=='left_only','Freeze']='After Freeze2 cutoff'
cobramergeFreezeforCobraAABC['PIN']=cobramergeFreezeforCobraAABC.subject+'_'+cobramergeFreezeforCobraAABC.redcap_event
cobramergeFreezeforCobraAABC.drop(columns=['_merge']).to_csv(outp+'testCobra.csv',index=False)

#now load the COBRA results to see what is there
Cobra=pd.DataFrame()
for studyshort in ['UCLA','WUSM','MGH','UMN']:
    box.downloadFile(config['NonQBox']['Cobras'][studyshort])
    WUCobra=pd.read_excel("./tmp/"+studyshort+"_actig_all.xlsx",sheet_name='Sheet1')
    x=list(WUCobra.columns)
    z=[y.upper() for y in x]
    WUCobra.columns=z
    WUCobra=WUCobra.rename(columns={"PARTICIPANT ID":"PIN"})
    WUCobra[['PIN','NIGHTS']]
    WUCobra=WUCobra.loc[WUCobra.NIGHTS.isnull()==False].copy()
    WUCobra['cobrasite']=studyshort
    Cobra=pd.concat([Cobra,WUCobra])

Cobra.cobrasite.value_counts()
Cobra.head()
Cobra['Cobra_Scored']='YES'
cobramergeFreezeforCobraAABC_Scored=pd.merge(cobramergeFreezeforCobraAABC,Cobra[['PIN','Cobra_Scored']],on='PIN',how='left')

clean=cobramergeFreezeforCobraAABC_Scored.drop(columns=['_merge','camntech_yn','Actigraphy','actigraphy_collectyn','actigraphy_partial_1___1','D'])
clean['priority']=''
clean.loc[clean.watch.isin((['PHILLIPS','CAMnTECH','BOTH'])) & (~(clean.Cobra_Scored=='YES')) & (~(clean.Freeze=='After Freeze2 cutoff')),'priority']=1

reorder=['subject','redcap_event','Site','event_date','Freeze','ActD_Freeze1','watch','Cobra_Scored','AnyActigraphyDataCollected','actigraphy_upload_typo','FailedScreen','priority']
#merge in known flags from Raiyan
raiyan=pd.read_csv(raiyanfile)
cleanraiyan=clean[reorder].merge(raiyan,on=['subject','redcap_event'],how='left')
cleanraiyan.loc[cleanraiyan.bad_data==1.0,'priority']=''

#merge in the diary info
report = redreport(tok=secret.loc[secret.source=='aabcarms','api_key'].reset_index().drop(columns='index').api_key[0],reportid='100205')
actdiary0=getframe(struct=report,api_url=config['Redcap']['api_url10'])
actdiary1=rollup(actdiary0)
actdiary2=actdiary1[[i for i in list(actdiary1.columns) if 'sad' not in i]+[i for i in list(actdiary1.columns) if 'sad' in i]]
actdiary3=actdiary2.loc[actdiary2.redcap_event.isin(['V1','V2','V3','V4'])].drop(columns=['study_id','subject_id','redcap_event_name','redcap_data_access_group', 'visit6','actigraphy_complete'])
actdiary4=actdiary3.drop(columns=['actigraphy_collectyn','actigraphy_upload_typo','actigraphy_upload_typoyn'])

#final reorder
reorderf=['subject','redcap_event','Site','event_date','Freeze','ActD_Freeze1','watch','Cobra_Scored','priority','comments','bad_data','AnyActigraphyDataCollected','camntech_yn','actigraphy_missreason','actigraphy_partial_1___1','actigraphy_partial_1___0','actigraphy_partial_1___2','actigraphy_partial_2___1','actigraphy_partial_2___0','actigraphy_partial_2___2','actigraphy_upload','actigraphy_upload_typo','actigraphy_upload_typo_details','actigraphy_upload_typo_s','actigraphy_upload_typo_u','general_note']
clean4cobra=cleanraiyan.merge(actdiary4,on=['subject','redcap_event'],how='left')
clean4cobra[reorderf+[o for o in clean4cobra.columns if 'sad' in o ]].to_csv(outp+'AABC_DataCurator_Actigraphy_Inventory.csv',index=False)


#Missing  : add exception for partial missing
missingAct=inventoryaabc2.loc[(inventoryaabc2.redcap_event_name.str.contains('v')) & (~(inventoryaabc2.Actigraphy=='YES'))]
missingAct.shape
#people that have partial data and it isknown that actigraphy device data are missing
partials=missingAct.loc[ (missingAct.actigraphy_collectyn == '2' ) & (missingAct.actigraphy_partial_1___1.astype('int')==0)][['PIN','actigraphy_collectyn','subject','actigraphy_partial_1___1']]
#don't want to include these guys in the list
missingAct=missingAct.loc[~missingAct.PIN.isin(list(partials.PIN))]
missingAct.shape

#drop the ones that are definitely nos, too.
missingAct=missingAct.loc[~(missingAct.actigraphy_collectyn == '0' )].copy()
missingAct.shape

#drop the ones that don't have subject ids
missingAct=missingAct.loc[~((missingAct.subject=='') | (missingAct.event_date==''))]
# %%
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

# %% [markdown]
# ## Consolidated Actigraphy stuff from Cobras

# %%
#NOW Do the Consolidated Actigraphy stuff from Cobras
# file = config['NonQBox']['Cobras'][studyshort]
#studyshort='WUSM'

#temp drop subject because of incomplete issue reporting with Cobra
#dropsubj=["HCA9226374_V3"]

# %%
Cobra=pd.DataFrame()
for studyshort in ['UCLA','WUSM','MGH','UMN']:
    box.downloadFile(config['NonQBox']['Cobras'][studyshort])
    WUCobra=pd.read_excel("./tmp/"+studyshort+"_actig_all.xlsx",sheet_name='Sheet1')
    x=list(WUCobra.columns)
    z=[y.upper() for y in x]
    WUCobra.columns=z
    WUCobra=WUCobra.rename(columns={"PARTICIPANT ID":"PIN"})
    WUCobra[['PIN','NIGHTS']]
    WUCobra=WUCobra.loc[WUCobra.NIGHTS.isnull()==False].copy()
    Cobra=pd.concat([Cobra,WUCobra])

# %%
#For now just capture typos, since collection status is checked elsewhere
#use fixes from above
#Cobra.PIN=Cobra.PIN.replace(fixes)
#Cobra=Cobra.loc[~(Cobra.PIN.isin(dropsubj))]
#Cobra.to_csv(outp+"tempclean_Cobra.csv",index=False)

# %%
#AllCobra=pd.merge(Cobra,inventoryaabc[['PIN']],how='outer',on='PIN',indicator=True)
#Bad IDs
#print("TRACK DOWN BAD IDS FROM COBRAS:",AllCobra.loc[AllCobra._merge=='left_only'])

# %% [markdown]
# ## MOCA SPANISH

# %%
#MOCA SPANISH  #############################################################
## no data yet

# %% [markdown]
# ## Psychopy

# %%
############################################################################
# Psychopy
# ORDER
# 1. scan for data in Box (here just looking for existence) and in IntraDB
# 2. QC ids (after incorporating patches)
# 3. generate an tickets and send to JIra if don't already exist
# 4. DONT dump or snapshot.  Leave data in IntraDB.

# %%
# maybe a different method of correcting typos is needed.  IntraDB needs to have correct information unless there is a patch somewhere.
# so need to create a patch for psychopy .. usually just one of the scans had a typo (e.g. vismotor but not resting state)

# %%
psychopy=process_files.loc[(process_files.filename.str.contains('psychopy'))].copy()
psychopy['datestamp']=psychopy.filename.str.split('_',expand=True)[2]
psychopy['datatype']=psychopy.filename.str.split('_',expand=True)[1]
psychopy=psychopy.loc[(psychopy.datestamp.str.contains('.csv')==True) & (psychopy.datestamp.str.contains('-'))].copy()
psychopy.datestamp=psychopy.datestamp.str.replace('.csv','')
psychopy.datestamp=pd.to_datetime(psychopy.datestamp)
psychopy=psychopy.sort_values('datestamp',ascending=False)
psychopy=psychopy.drop_duplicates(subset='datatype',keep='first').copy()

# %%
psychopy

# %%
psychintradb=process_files.loc[(process_files.filename.str.contains('psychintradb'))].copy()
psychintradb['datestamp']=psychintradb.filename.str.split('_',expand=True)[2]
psychintradb['datatype']=psychintradb.filename.str.split('_',expand=True)[1]
psychintradb=psychintradb.loc[(psychintradb.datestamp.str.contains('.csv')==True) & (psychintradb.datestamp.str.contains('-'))].copy()
psychintradb.datestamp=psychintradb.datestamp.str.replace('.csv','')
psychintradb.datestamp=pd.to_datetime(psychintradb.datestamp)
psychintradb=psychintradb.sort_values('datestamp',ascending=False)
psychintradb=psychintradb.drop_duplicates(subset='datatype',keep='first').copy()

# %%
psychintradb

# %%
anydata=pd.read_csv(box.downloadFile(psychopy['fileid'][0]),low_memory=False,encoding='ISO-8859-1')

# %%
#box scan moved to cron
#anydata=pd.read_csv(outp+"temp_psychopy.csv")
anydata.columns=['subject','redcap_event','scan','fname','']
anydata.loc[~(anydata.subject.isin(["HCA1234567"]))]

PSY=anydata[['subject','redcap_event']].drop_duplicates().copy()
checkIDB=anydata[['subject','redcap_event','scan']].copy()
checkIDB['PIN_AB']=checkIDB.subject+'_'+checkIDB.redcap_event + '_'+checkIDB.scan
ci=checkIDB.drop_duplicates(subset='PIN_AB')

#sent intradb scan to psychopy
df=pd.read_csv(box.downloadFile(psychintradb['fileid'][0]),low_memory=False,encoding='ISO-8859-1')
df.columns = ['PIN_AB']
df.PIN_AB=df.PIN_AB.str.replace('t','').str.strip()

# %%
psymiss=pd.merge(ci,df, on='PIN_AB',how='outer',indicator=True).drop_duplicates()
p1=pd.DataFrame()
if psymiss.loc[psymiss._merge=='left_only'].shape[0]>0:
    p1 = pd.DataFrame(psymiss.loc[psymiss._merge=='left_only'].PIN_AB.unique())
    p1=p1.loc[~((p1[0].isnull()==True) | ((p1[0].str.contains("HCA678899_V3"))==True) | ((p1[0].str.contains("HCA996201_V3"))==True) | ((p1[0].str.contains("HCA752775_V3"))==True))].copy()  #invalid PINs.  Probably not necessary exclusion seeing as merging with inventory
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

# %%
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


# %%
#dont worry about duplicates in IntraDB - these will be filtered.
#find subjects in AABC but not in IntraDB or BOX
# scan_collectyn = 2 ->
psymiss['redcap_event'] = psymiss['redcap_event'].str.strip()
PSY2=psymiss.drop_duplicates(subset=['subject','redcap_event'])[['subject','redcap_event']]
PSY2['Psychopy']='YES'
inventoryaabc7=pd.merge(inventoryaabc,PSY2,on=['subject','redcap_event'],how='left')
missingPY=inventoryaabc7.loc[(inventoryaabc7.redcap_event_name.str.contains('v')) & (~(inventoryaabc7.Psychopy=='YES'))].copy()
dropm=inventoryaabc7.loc[(inventoryaabc7.missscan4=='1') | (inventoryaabc7.missscan4=='2') | (inventoryaabc7.scan_collectyn=='0') | (inventoryaabc7.scan_collectyn=='2')][['PIN']]


# %%
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


# %%
P=pd.concat([pwho,peepy])
#IntraDB ID
P=P[['subject','redcap_event','study_id', 'site','reason','code','issueCode','v0_date','event_date','datatype']]
P=P.drop_duplicates()
#no psychopy data:
P=P.loc[~((P.subject=="HCA6487286") & (P.redcap_event=='V3'))]

# %% [markdown]
# ## HOT FLASH DATA

# %%
##################################################################################
#HOT FLASH DATA
#NOTE: Maki's group to do the scoring and stuff.  We are just checking for IDs and typos
#HotFiles=pd.read_csv(outp+"temp_Hotties.csv")
#Hotties=pd.merge(inventoryaabc,HotFiles,on='PIN',how='outer',indicator=True)

# %%
#now get copies of HCA restricted REDCap, Q


#whittle down the list of files in the Pre-Release folder to the most recent subset
#process_files=pd.DataFrame.from_dict(aabc_processing, orient='index')
hotflash_files=process_files.loc[(process_files.filename.str.contains('hotflash'))].copy()
hotflash_files['datestamp']=hotflash_files.filename.str.split('_',expand=True)[2]
hotflash_files['datatype']=hotflash_files.filename.str.split('_',expand=True)[1]
hotflash_files=hotflash_files.loc[(hotflash_files.datestamp.str.contains('.csv')==True) & (hotflash_files.datestamp.str.contains('-'))].copy()
hotflash_files.datestamp=hotflash_files.datestamp.str.replace('.csv','')
hotflash_files.datestamp=pd.to_datetime(hotflash_files.datestamp)
hotflash_files=hotflash_files.sort_values('datestamp',ascending=False)
hotflash_files=hotflash_files.drop_duplicates(subset='datatype',keep='first').copy()

# %%
hotflash_files

# %%
#HotFiles=pd.read_csv(box.downloadFile(config['hotfiles']),low_memory=False,encoding='ISO-8859-1')
HotFiles=pd.read_csv(box.downloadFile(hotflash_files['fileid'][0]),low_memory=False,encoding='ISO-8859-1')
Hotties=pd.merge(inventoryaabc,HotFiles,on='PIN',how='outer',indicator=True)

# %%
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

# %%
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

# %% [markdown]
# ## CHECK key REDCap AABC variables for completeness

# %%
###################################################################################
# To DO: Forgot to CHECK FOR BUNK IDS IN PSYCHOPY AND ACTIGRAPHY
###################################################################################

# %%
###################################################################################
# NOW CHECK key REDCap AABC variables for completeness (counterbalance, inventory completeness, age, bmi and soon to be more)
# inventory_complete
pd.set_option('display.width', 1000)
pd.options.display.width=1000

# %%
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

# %%
summv=inventoryaabc.loc[inventoryaabc.redcap_event_name.astype('str').str.contains('v')][['study_id','site','subject','redcap_event','visit_summary_complete','event_date']]
summv=summv.loc[~(summv.visit_summary_complete=='2')]
if summv.shape[0]>0:
    summv['code']='GREEN'
    summv['issueCode']='AE2001'
    summv['datatype']='REDCap'
    summv['reason']='Visit Summary Incomplete'
    summv=summv[['subject','redcap_event','study_id', 'site','reason','code','issueCode','event_date','datatype']]
    print("Visit Summary Incomplete:\n",summv)

# %%
#
#agev=inventoryaabc7.loc[inventoryaabc7.redcap_event_name.astype('str').str.contains('v')][['redcap_event', 'study_id', 'site','subject','redcap_event_name','age_visit','event_date','v0_date']]
#ag=agev.loc[agev.age_visit !='']
#agemv=ag.loc[(ag.age_visit.astype('float')<=36) | (ag.age_visit.astype('float')>=95 )].copy()
#if agemv.shape[0]>0:
#    print("AGE OUTLIERS:\n",agemv)
#    agemv['code']='RED'
#    agemv['issueCode']='AE7001'
#    agemv['datatype']='REDCap'
#    agemv['reason']='Age outlier. Please double check DOB and Event Date'
#    agemv=agemv[['subject','redcap_event','study_id', 'site','reason','issueCode','code','event_date','v0_date','datatype']]

#aa=agev.loc[agev.age_visit=='']
#ab=agev.loc[~(agev.age_visit=='')]
#ageav=pd.concat([aa,ab.loc[(ab.age_visit.astype(float).isnull()==True)]])
#if ageav.shape[0]>0:
#    print("Missing Age:\n",ageav)
#    ageav['code']='RED'
#    ageav['datatype']='REDCap'
#    ageav['issueCode']='AE3001'
#    ageav['reason']='Missing Age. Please check DOB and Event Date'
#    ageav=ageav[['subject','redcap_event','study_id', 'site','reason','issueCode','code','event_date','v0_date','datatype']]

# %%
#calculate BMI: weight (lb) / [height (in)]2 x 703
bmiv=inventoryaabc.loc[inventoryaabc.redcap_event_name.astype('str').str.contains('v')][['bmi','redcap_event','subject','study_id', 'site','event_date','height_outlier_jira','weight_outlier_jira', 'height_missing_jira','bmi_outlier']].copy()
bmiv.loc[bmiv.subject=='HCA8953805']

#outliers

#ADD in Check for moca_total
print("Moca Values:",inventoryaabc.moca_sum.value_counts(dropna=False))

#add in check against the extreme value confirmation and then relax the extremes
a=bmiv.loc[~(bmiv.bmi=='')].copy()
a=a.loc[(a.bmi.astype('float')<=18.0) | (a.bmi.astype('float')>=40)].copy()
a=a.loc[~((a.height_outlier_jira==1) | (a.weight_outlier_jira) | (a.bmi_outlier=='1'))]
if a.shape[0]>0:
    print("BMI OUTLIERS:\n", a.loc[(a.bmi.astype('float') <= 18) | (a.bmi.astype('float') >= 40)])
    a['code']='PINK'
    a['datatype']='REDCap'
    a['reason']='BMI is an outlier.  Please double check height and weight'
    a['issueCode']='AE7001'
    a=a[['subject','redcap_event','study_id', 'site','reason','code','issueCode','event_date','datatype']]

# %%
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

# %%
#check counterbalance distribution - 97 vs 82 (3 to 4) as o 1/27/23
inventoryaabc.loc[inventoryaabc.redcap_event_name.astype('str').str.contains('register')].counterbalance_1st.value_counts()

#add in warnings about psqi variables
#this is done...instead of catching this way, need to catch on the fly, since its a survey
#psqitemp=pd.read_csv(outp+"psqiflagsTemp.csv")
#psqitemp['datatype']='REDCap'
#psqitemp['code']='RED'
#psqitemp['issueCode']='AE1001'
#psqis=pd.merge(inventoryaabc[['subject','study_id','redcap_event_name','redcap_event','site','event_date']],psqitemp,on=['study_id','redcap_event_name'],how='right')
##############################################################################
#####

# %% [markdown]
# ## Joined JIRA ticket list

# %%
#all the flags for JIRA together
#QAAP=concat(Q1,Q2,a1,a11,a13,a14,a2,P,C,summv,agemv,ageav,a, bmiv,T,Hot1,Hot2b).drop(columns=['v0_date'])
# Q1:mian check 
# Q2:Q-interactive
# a1,a11,a13,a14: ASA24
# a2: actiscan
# P: psychopy
# C,summv,a,agemv,ageav,bmiv: key REDCap aabc
# T: toolbox
# Hot1,Hot2b: hotflash
#QAAP=concat(Q1,C,summv,a, bmiv,T).drop(columns=['v0_date'])
#QAAP=concat(a2).drop(columns=['v0_date'])
#QAAP=concat(a1,a11,a13,a14).drop(columns=['v0_date'])
QAAP=concat(Q2,T,a1,a11,a13,a14,C).drop(columns=['v0_date'])
#QAAP=Q2.copy()

# %%

#Drop this screenfailure from inventory and issues
#HCA6276071 V1
#make sure screenfailures and DNR aren't included.
#DNRsubs = ["HCA7787304", "HCA6276071", "HCA6229365", "HCA9191078", "HCA6863086","HCA9515381"]
DNRsubs = ['HCA8151667','HCA7486188','HCA8421569','HCA8565694','HCA7665087','HCA8456790','HCA8476190','HCA7410559','HCA7908188','HCA6640672','HCA6223050','HCA6909286','HCA9061974','HCA9944304','HCA7030751','HCA9726192','HCA9662900','HCA9384089','HCA9833395','HCA7016050','HCA9017668','HCA9198294','HCA7996111','HCA9864811','HCA9071068','HCA9482089','HCA8187587','HCA8816592','HCA7982706','HCA7698204','HCA7256979','HCA7248879','HCA6570677','HCA8931693','HCA7106657','HCA6224355','HCA7291678','HCA6318465','HCA7316870','HCA7670989','HCA8532174','HCA9769211','HCA6143557','HCA8238275','HCA7153161','HCA7195884','HCA7385889','HCA9262277','HCA6785193','HCA9845403','HCA8363278','HCA6472071','HCA9154375','HCA7397391','HCA8232061','HCA7271369','HCA9449900','HCA6910372','HCA8378594','HCA7243263','HCA6631671','HCA9630684','HCA7891703','HCA6700664','HCA7025253','HCA7604067','HCA7738290','HCA7764594','HCA6791794','HCA9086990','HCA6421963','HCA8937807','HCA8912588','HCA9938309','HCA7655892','HCA7625176','HCA7382479','HCA8317675','HCA6120646','HCA7126663','HCA6946191','HCA8868915','HCA6224355']

QAAP=QAAP.loc[~(QAAP.subject.isin(DNRsubs))].copy()
QAAP=QAAP.loc[~(QAAP.event_date=='')].copy()
QAAP=QAAP.loc[~(QAAP.event_date.isnull()==True)]
QAAP=QAAP.loc[~(QAAP.subject=='')].copy()
QAAP=QAAP.loc[~(QAAP.site=='')].copy()

QAAP['QCdate'] = date.today().strftime("%Y-%m-%d")
QAAP['issue_age']=(pd.to_datetime(QAAP.QCdate) - pd.to_datetime(QAAP.event_date))
QAAP=QAAP[['subject','redcap_event','study_id', 'site','reason','code','issueCode','event_date','issue_age','datatype']].drop_duplicates()
QAAP.sort_values(['site','issue_age'],ascending=False).to_csv('All_Issues_'+date.today().strftime("%d%b%Y")+'.csv',index=False)


# %%
#QAAP

# %%
###REDUCE by Color code.... need to be able to change these values.
filteredQ=QAAP.loc[((QAAP.code=='PINK') & (QAAP.issue_age.dt.days>7)) | ((QAAP.code=='RED') & (QAAP.issue_age.dt.days>4)) | ((QAAP.code=='RED') & (QAAP.issue_age.dt.days.isnull()==True)) |  ((QAAP.code=='ORANGE') & (QAAP.issue_age.dt.days>18)) |  ((QAAP.code=='YELLOW') & (QAAP.issue_age.dt.days>36)) |  ((QAAP.code=='GREEN') & (QAAP.issue_age.dt.days>45)) ].copy()
#filteredQ['issue_age']=filteredQ.issue_age.astype('str').str.split(expand=True)[0]

filteredQ.to_csv('FilteredQC4Jira.csv',index=False)
#filteredQ=pd.read_csv('FilteredQC4Jira.csv')

# %%
filteredQ

# %%
# Convert 'issue_age' from string to integer
filteredQ['issue_age'] = filteredQ['issue_age'].astype(str)
filteredQ['issue_age'] = filteredQ['issue_age'].str.extract('(\d+)').astype(int)

# %%
show(
    filteredQ,
    buttons=["copyHtml5", "csvHtml5", "excelHtml5"],
    layout={"top1": "searchPanes"},
    searchPanes={"layout": "columns-3", "cascadePanes": True},
)

# %%
# Define a threshold for old tickets (e.g., tickets older than 365 days)
old_ticket_threshold = 300

# Create the crosstab table
table = pd.crosstab(filteredQ['site'], filteredQ['datatype'])

# Function to apply annotation
def annotate_old_tickets(site, datatype):
    mask = (filteredQ['site'] == site) & (filteredQ['datatype'] == datatype)
    if any(filteredQ.loc[mask, 'issue_age'] > old_ticket_threshold):
        return f'{table.loc[site, datatype]}*'
    else:
        return table.loc[site, datatype]

# Annotate the table
annotated_table = table.apply(lambda col: col.index.to_series().apply(lambda row: annotate_old_tickets(row, col.name)))

# Display the annotated table
annotated_table


