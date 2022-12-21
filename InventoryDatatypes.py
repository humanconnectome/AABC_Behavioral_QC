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
config = LoadSettings()
secret=pd.read_csv(config['config_files']['secrets'])
box = LifespanBox(cache="./tmp")

## get the HCA inventory for ID checking with AABC
pathp=box.downloadFile(config['hcainventory'])

#get current version variable mask from BOX (for excluding variables just prior to sending snapshots to PreRelease for investigator access)
#this is not yet working
a=box.downloadFile(config['variablemask'])
rdrop=getlist(a,'AABC-ARMS-DROP')
rrest=getlist(a,'AABC-ARMS-RESTRICTED')
rraw=getlist(a,'TLBX-RAW-RESTRICTED')
rscore=getlist(a,'TLBX-SCORES-RESTRICTED')

#get ids
ids=pd.read_csv(pathp)
hcaids=ids.subject.drop_duplicates()
#for later use in getting the last visit for each participant in HCA so that you can later make sure that person is starting subsequent visit and not accidentally enrolled in the wrong arm
hca_lastvisits=ids[['subject','redcap_event']].loc[ids.redcap_event.isin(['V1','V2'])].sort_values('redcap_event').drop_duplicates(subset='subject',keep='last')


#########################################################################################
#PHASE 0 TEST IDS AND ARMS
# if Legacy, id exists in HCA and other subject id related tests:
#Test that #visits in HCA corresponds with cohort in AABC

#construct the json that gets sent to REDCap when requesting data.
#all data (not actually getting these now)
aabcarms = redjson(tok=secret.loc[secret.source=='aabcarms','api_key'].reset_index().drop(columns='index').api_key[0])
hcpa = redjson(tok=secret.loc[secret.source=='hcpa','api_key'].reset_index().drop(columns='index').api_key[0])
#just a report
aabcreport = redreport(tok=secret.loc[secret.source=='aabcarms','api_key'].reset_index().drop(columns='index').api_key[0],reportid='51031')

#download the inventory report from AABC for comparison
aabcinvent=getframe(struct=aabcreport,api_url=config['Redcap']['api_url10'])
#aabcarmsdf=getframe(struct=aabcarms,api_url=config['Redcap']['api_url10'])

#trying to set study_id from config file, but have been sloppy...there are instances where the actual subject_id has been coded below
study_id=config['Redcap']['datasources']['aabcarms']['redcapidvar']

#slim selects just the registration event (V0) because thats where the ids and legacy information is kept.
slim=aabcinvent[['study_id','redcap_event_name',study_id,'legacy_yn','site','v0_date']].loc[(aabcinvent.redcap_event_name.str.contains('register'))]

#compare aabc ids against hcaids and whether legacy information is properly accounted for (e.g. legacy variable flags and actual event in which participannt has been enrolled.
fortest=pd.merge(hcaids,slim,left_on='subject',right_on=study_id,how="outer",indicator=True)
#fortest._merge.value_counts()
legacyarms=['register_arm_1','register_arm_2','register_arm_3','register_arm_4','register_arm_5','register_arm_6','register_arm_7','register_arm_8']

# First batch of flags: Look for legacy IDs that don't actually exist in HCA
#send these to Angela for emergency correction after dropping rows with missing ids or test subjects:
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
wrongvisit=wrongvisit.loc[~(wrongvisit.redcap_event_name=='phone_call_arm_13')]
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
            print('CODE RED (if HCA691778 ignore) :',s3,': Subject found in AABC REDCap Database initiating the wrong visit sequence (e.g. V3 insteady of V2')

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
    Q1 = concat(*[Q0,qlist1,qlist2,qlist3,qlist4,qlist5])
except:
    Q1=pd.DataFrame(columns=['subject_id', 'study_id', 'redcap_event_name', 'site','reason','issue_code','code','v0_date','event_date'])
#########################################################################################


#########################################################################################
# PHASE 1 Test that all dataypes expected are present
# Get the REDCap AABC inventory (which may or may not agree with the reality of data found):
#there are lots of variables in the inventory.  Don't need them all
keeplist=['study_id','redcap_event_name','v0_date','dob','age','sex','legacy_yn','psuedo_guid',
          'ethnic','racial','site','passedscreen','subject_id','counterbalance_1st','counterbalance_2nd','height_ft', 'height_in', 'weight','bmi', 'height_outlier_jira', 'height_missing_jira',
          'age_visit','event_date','completion_mocayn','ravlt_collectyn','nih_toolbox_collectyn','nih_toolbox_upload_typo',
          'tlbxwin_dups_v2','walkendur_dups','actigraphy_collectyn','actigraphy_partial_1','actigraphy_upload_typo','actigraphy_upload_typoyn',
          'vms_collectyn','face_complete','visit_summary_complete','asa24yn','asa24id']

inventoryaabc=idvisits(aabcinvent,keepsies=keeplist)
inventoryaabc = inventoryaabc.loc[~(inventoryaabc.subject_id.str.upper().str.contains('TEST'))].copy()

# FLOW:
# Qinteractive  order:
# # 1. grab new stuff from box
# # 2. transform it
# # 3. send it to REDCap
# # 4. QC (incorporating patches)
# # 5. generate tickets
# # 6. send tickets that arent identical to ones already in Jira (now or at the end in a single bolus)
# # 7. create and send snapshot of patched data to BOX after dropping restricted variables

# Observed:
# pull Q data from Box to qint REDCap, then query qint against AABC-Arms study ids and visit
#    All records EVER created will be included in REDCap.
#    duplications
#    typos will be set to unusable automatically
#    missing: look for potential records in REDCap, first.  Correct in REDCap Not BOX or it will lead to duplicate.
#    if dup, set one to unususable and explain

#the variables that make up the 'common' form in the Qinteractive database.
firstvarcols = ['id', 'redcap_data_access_group', 'site', 'subjectid', 'fileid',
                'filename', 'sha1', 'created', 'assessment', 'visit', 'form',
                'q_unusable', 'unusable_specify', 'common_complete', 'ravlt_two']

#the variables that make up the ravlt form
columnnames = ['ravlt_pea_ravlt_sd_tc', 'ravlt_delay_scaled', 'ravlt_delay_completion', 'ravlt_discontinue',
               'ravlt_reverse', 'ravlt_pea_ravlt_sd_trial_i_tc', 'ravlt_pea_ravlt_sd_trial_ii_tc',
               'ravlt_pea_ravlt_sd_trial_iii_tc', 'ravlt_pea_ravlt_sd_trial_iv_tc', 'ravlt_pea_ravlt_sd_trial_v_tc',
               'ravlt_pea_ravlt_sd_listb_tc', 'ravlt_pea_ravlt_sd_trial_vi_tc', 'ravlt_recall_correct_trial1',
               'ravlt_recall_correct_trial2', 'ravlt_recall_correct_trial3', 'ravlt_recall_correct_trial4',
               'ravlt_delay_recall_correct', 'ravlt_delay_recall_intrusion', 'ravlt_delay_total_intrusion',
               'ravlt_delay_total_repetitions']


#current Qint Redcap:
qintreport = redreport(tok=secret.loc[secret.source=='qint','api_key'].reset_index().drop(columns='index').api_key[0],reportid='51037')
qintdf=getframe(struct=qintreport,api_url=config['Redcap']['api_url10'])

#all box files - grab, transform, send
folderqueue=['WU','UMN','MGH'] ###,'UCLA']
######
###THIS WHOLE SECTION NEEDS TO BE CRON'D - e.g. scan for anything new and import it into Qinteractive - let patch in REDCap handle bad or duplicate data.
#this is currently taing too much time to iterate through box
#import anything new by any definition (new name, new sha, new fileid)
studyshortsum=0
all2push=pd.DataFrame()
for studyshort in folderqueue:
    folder = config['Redcap']['datasources']['qint']['BoxFolders'][studyshort]
    dag = config['Redcap']['datasources']['aabcarms'][studyshort]['dag']
    sitenum = config['Redcap']['datasources']['aabcarms'][studyshort]['sitenum']

    filelist=box.list_of_files([folder])
    print(filelist)
    #print(filelist)
    db=pd.DataFrame(filelist).transpose()#.reset_index().rename(columns={'index':'fileid'})
    print(db)
    db.fileid=db.fileid.astype(int)
    #print(studyshort,":",db.shape[0],"records in BOX")
    #ones that already exist in q redcap
    cached_filelist=qintdf.copy()
    cached_filelist.fileid=cached_filelist.fileid.astype('Int64') #ph.asInt(cached_filelist, 'fileid')

    #find the new ones that need to be pulled in
    newfileids=pd.merge(db,cached_filelist.fileid,on='fileid',how='left',indicator=True)
    newfileids_temp=newfileids.copy()
    newfileids=newfileids.loc[newfileids._merge=='left_only'].drop(columns=['_merge'])
    db2go=db.loc[db.fileid.isin(list(newfileids.fileid))]
    if db2go.empty:
        print("NO NEW RECORDS from",studyshort,"TO ADD AT THIS TIME")
    if not db2go.empty:
        print("you are here")
        #initiate new ids
        #NEED TO UPDATE CACHED_FILELIST.ID after every iteration.
        s = cached_filelist.id.astype('Int64').max() + 1 + studyshortsum
        l=len(db2go)
        vect=[]
        for i in range(0,l):
            id=i+s
            vect=vect+[id]
        studyshortsum=studyshortsum+l
        print("now you are here")
        rows2push=pd.DataFrame(columns=firstvarcols+columnnames)
        for i in range(0,db2go.shape[0]):
            try:
                redid=vect[i]
                fid=db2go.iloc[i][['fileid']][0]
                print("fid",fid)
                t=box.getFileById(fid)
                created=t.get().created_at
                fname=db2go.iloc[i][['filename']][0]
                subjid = fname[fname.find('HCA'):10]
                fsha=db2go.iloc[i][['sha1']][0]
                print(i)
                print(db2go.iloc[i][['fileid']][0])
                print(db2go.iloc[i][['filename']][0])
                print(db2go.iloc[i][['sha1']][0])
                print("subject id:",subjid)
                print("Redcap id:",redid)
                #pushrow=getrow(fid,fname)
                content=box.read_text(fid)
                assessment='RAVLT'
                if 'RAVLT-Alternate Form C' in content:
                            form = 'Form C'
                if 'RAVLT-Alternate Form D' in content:
                            form = 'Form D'
                if fname.find('Form B')>0:
                            form= 'Form B'
                #visits = sorted(list(map(int,requests.findall('[vV](\d)', fname))))
                a = fname.replace("AV", "").find('V')
                visit=fname[a+1]
                #visit=visits[-1]
                row=parse_content(content)
                df = pd.DataFrame([row], columns=columnnames)
                #print(df)
                firstvars = pd.DataFrame([[redid, dag, sitenum, subjid, fid, fname, fsha, created, assessment,
                                           visit, form, "", "", "", ""]], columns=firstvarcols)
                #print(firstvars[['filename','subjectid']])
                pushrow=pd.concat([firstvars,df],axis=1)
                #print(pushrow)
                rows2push=pd.concat([rows2push,pushrow],axis=0)
            except:
                print("something wrong...moving along")
        if len(rows2push.subjectid) > 0:
            print("*************",studyshort,"  **********************")
            print(len(rows2push.subjectid),"rows to push:")
            print(list(rows2push.subjectid))
            print("***************************************************")
        all2push=pd.concat([all2push,rows2push]).drop_duplicates()
print("**************** Summary All Sites **********************")
print(len(all2push.subjectid),"Total rows to push across sites:")

if not all2push.empty:
  send_frame(dataframe=all2push, tok=secret.loc[secret.source=='qint','api_key'].reset_index().drop(columns='index').api_key[0])
####
###END SECTION THAT NEEDS TO BE TURNED INTO A CRON JOB


#QC checks
#now check first define PIN for merging and drop unusables
qintdf2=getframe(struct=qintreport,api_url=config['Redcap']['api_url10'])
invq=qintdf2[['id', 'site', 'subjectid','visit','q_unusable']].copy()
invq['redcap_event']="V"+invq.visit
invq['Qint']='YES'
#invq=invq.loc[~(invq.subjectid.str.upper().str.contains('TEST'))]
invq=invq.loc[~(invq.q_unusable=='1')].copy()

#Before merging, check for duplicates that haven't been given the 'unusable' flag
dups=qintdf.loc[qintdf.duplicated(subset=['subjectid','visit'])]
dups2=dups.loc[~(dups.q_unusable.isnull()==False)]  #or '', not sure

q0=pd.DataFrame()
if dups2.shape[0]>0:
    print("Duplicate Q-interactive records")
    print(dups2[['subjectid','visit']])
    q0['datatype']='RAVLT'
    q0['reason']=['Duplicate Q-interactive records']
    q0['code']='ORANGE'
    q0['issueCode']='AE5001'

#inventoryaabc2=pd.merge(inventoryaabc,invq.rename(columns={'subjectid':'subject'}).drop(columns=['site']),on=['subject','redcap_event'],how='outer',indicator=True)
inventoryaabc2=pd.merge(inventoryaabc,invq.drop(columns=['site']),left_on=['subject','redcap_event'],right_on=['subjectid','redcap_event'],how='outer',indicator=True)

#find ids (including test subjects) that aren't i nmain AABC (i.e. probably typos)
#These all need to go to Petra - and documented in patch
q1=pd.DataFrame()
if inventoryaabc2.loc[inventoryaabc2._merge=='right_only'].shape[0] > 0 :
    print("The following ID(s)/Visit(s) are not found in the main AABC-ARMS Redcap.  Please investigate")
    print(inventoryaabc2.loc[inventoryaabc2._merge=='right_only'][['subject','redcap_event']])
    q1=inventoryaabc2.loc[inventoryaabc2._merge=='right_only'][['subject','redcap_event']]
    q1['reason']=['Subject with Q-int data but ID(s)/Visit(s) are not found in the main AABC-ARMS Redcap.  Please look for typo']
    q1['code']='ORANGE'
    q1['issueCode']='AE1001'
    q1['datatype']='RAVLT'

#inventoryaabc2._merge.value_counts()
inventoryaabc3=inventoryaabc2.loc[inventoryaabc2._merge!='right_only'].drop(columns=['_merge'])
#inventoryaabc2.to_csv('test.csv',index=False)

#this isn't working.  why not?  see AABCMGH-2
missingQ=inventoryaabc3.loc[(inventoryaabc2.redcap_event_name.str.contains('v')) & (~(inventoryaabc2.Qint=='YES'))][['subject_id','study_id','subject','redcap_event','site','event_date']]
q2=pd.DataFrame()
if missingQ.shape[0]>0:
    print("Q-interactive cannot be found for")
    print(missingQ)
    q2=missingQ.copy()
    q2['reason']='MISSING or MALFORMED Q-interactive data for this subject/visit'
    q2['code']='ORANGE'
    q2['issueCode']='AE4001'
    q2['datatype']='RAVLT'

Q0=pd.DataFrame(columns=['subject_id', 'study_id', 'redcap_event_name', 'site','reason','issueCode','code','v0_date','event_date','datatype'])
Q2=pd.concat([Q0,q0,q1,q2],axis=0)
Q2['subject_id']=Q2.subject

# pseudo code for snapshot to BOX
# Send to Box
# drop all the q_unusable.isnull()==False) records, as well as any ids that are not in AABC redcap
# drop restricted vars.
# send to BOX - this code needs to be fixed, but it will work
#qA,qArestricted=getredcap10Q('qint',Asnaps,list(inventoryaabc.subject.unique()),'AABC',restrictedcols=restrictedQ)
##idstring='Q-Interactive'
##studystr='AABC'
##qA.drop(columns='unusable_specify').to_csv(box_temp + '/' + studystr + '_' + idstring + '_' + snapshotdate + '.csv', index=False)
##qArestricted.drop(columns='unusable_specify').to_csv(box_temp + '/' + studystr + '_' + idstring + '_Restricted_' + snapshotdate + '.csv', index=False)
##box.upload_file(box_temp + '/' + studystr + '_' + idstring + '_' + snapshotdate + '.csv', Asnaps)
##box.upload_file(box_temp + '/' + studystr + '_' + idstring + '_Restricted_' + snapshotdate + '.csv', ArestrictSnaps)


# NOW FOR TOOLBOX. ############################################################################
# # 1. grab partial files from intraDB
# # 2. QC (after incorporating patches)
# # 3. generate tickets and send to JIra if don't already exist
# # 4. send tickets that arent identical to ones already in Jira
# # 5. concatenate legit data (A scores file and a Raw file, no test subjects or identical duplicates -- no 'Narrow' or 'Registration' datasets)
# # 6. create and send snapshot of patched data to BOX after dropping restricted variables

##FIRST THE RAW DATA FILES
rawd4 = run_ssh_cmd('plenzini@login3.chpc.wustl.edu',
                    'find /ceph/intradb/archive/AABC_WU_ITK/resources/toolbox_endpoint_data/ -type f  ! \( -name "*Scores*" -o -name "*Narrow*" -o -name "*Regist*" -o -name "*catalog*" \) -exec cat {} \;').stdout.read()
rawd1 = run_ssh_cmd('plenzini@login3.chpc.wustl.edu',
                    'find /ceph/intradb/archive/AABC_MGH_ITK/resources/toolbox_endpoint_data/ -type f  ! \( -name "*Scores*" -o -name "*Narrow*" -o -name "*Regist*" -o -name "*catalog*" \) -exec cat {} \;').stdout.read()
rawd3 = run_ssh_cmd('plenzini@login3.chpc.wustl.edu',
                    'find /ceph/intradb/archive/AABC_UMN_ITK/resources/toolbox_endpoint_data/ -type f  ! \( -name "*Scores*" -o -name "*Narrow*" -o -name "*Regist*" -o -name "*catalog*" \) -exec cat {} \;').stdout.read()
rawd2 = run_ssh_cmd('plenzini@login3.chpc.wustl.edu',
                    'find /ceph/intradb/archive/AABC_UCLA_ITK/resources/toolbox_endpoint_data/ -type f  ! \( -name "*Scores*" -o -name "*Narrow*" -o -name "*Regist*" -o -name "*catalog*" \) -exec cat {} \;').stdout.read()
#note that some of these won't work because UCLA hasn't started collecting data
raw41=TLBXreshape(rawd4)
raw11=TLBXreshape(rawd1)
raw31=TLBXreshape(rawd3)
raw21=TLBXreshape(rawd2)

#remove files known to be duds.
rf2=pd.concat([raw41,raw31,raw21,raw11])
rf2=rf2.loc[~(rf2.PIN.str.upper().str.contains('TEST'))]
rf2=rf2.loc[~(rf2.PIN.str.upper()=='ABC123')]

#drop duplicates for purpose of generating QC flags.
rf2=rf2.drop_duplicates(subset='PIN').copy()

#fixtypos - NEED TO incorporate information about date of session as given in filename because of typos involving legit ids
# THERE ARE TWO SUBJECTS HERE WHOSE NEXT VISIT WILL BE IN CONFLICT WITH THIS ONE, OTHERWISE
fixtypos=inventoryaabc2.loc[inventoryaabc2.nih_toolbox_upload_typo!=''][['subject','redcap_event','nih_toolbox_upload_typo']]
fixtypos['PIN']=fixtypos.subject+'_'+fixtypos.redcap_event
fixes=dict(zip(fixtypos.nih_toolbox_upload_typo, fixtypos.PIN))
rf2.PIN=rf2.PIN.replace(fixes)

#NOW THE SCORED DATA
results4 = run_ssh_cmd('plenzini@login3.chpc.wustl.edu',
                       'cat /ceph/intradb/archive/AABC_WU_ITK/resources/toolbox_endpoint_data/*Scores* | cut -d"," -f1,2,3,4,10 | sort -u').stdout.read()
results1 = run_ssh_cmd('plenzini@login3.chpc.wustl.edu',
                       'cat /ceph/intradb/archive/AABC_MGH_ITK/resources/toolbox_endpoint_data/*Scores* | cut -d"," -f1,2,3,4,10 | sort -u').stdout.read()
results3 = run_ssh_cmd('plenzini@login3.chpc.wustl.edu',
                       'cat /ceph/intradb/archive/AABC_UMN_ITK/resources/toolbox_endpoint_data/*Scores* | cut -d"," -f1,2,3,4,10 | sort -u').stdout.read()
results2 = run_ssh_cmd('plenzini@login3.chpc.wustl.edu',
                       'cat /ceph/intradb/archive/AABC_UCLA_ITK/resources/toolbox_endpoint_data/*Scores* | cut -d"," -f1,2,3,4,10 | sort -u').stdout.read()

#  TWO 'NIGHTMARE' SUBJECTS so far.  Removed bad data by hand from IntraDB.  fixed, uploaded bad data (provenance) to REDCap.  Uploaded corrected data to IntraDB
#  HCA8596099_V3 and HCA7802172_V3 (both should be V2).  Typo listed as 'NIGHTMARE' in the visit summary.

dffull1=TLBXreshape(results1)
dffull2=TLBXreshape(results2)
dffull3=TLBXreshape(results3)
dffull4=TLBXreshape(results4)

##fixtypos in Scores file now
dffull=pd.concat([dffull1, dffull3, dffull2, dffull4])
dffull=dffull.copy() #pd.concat([df11,df31])
dffull=dffull.loc[~(dffull.PIN.str.upper().str.contains('TEST'))]
dffull=dffull.loc[~(dffull.PIN.str.upper()=='ABC123')]
dffull.PIN=dffull.PIN.replace(fixes)

#merge with patch fixes (i.e. delete duplicate specified in visit summary)
# This is a single fix... need to generalized to all instruments and their corresponding dupvars:
# -->HCA8596099_V3 has 2 assessments for Words in Noise - add patch note"
iset=inventoryaabc2
dffull=filterdupass('NIH Toolbox Words-In-Noise Test Age 6+ v2.1','tlbxwin_dups_v2',iset,dffull)
dffull=filterdupass('NIH Toolbox 2-Minute Walk Endurance Test Age 3+ v2.0','walkendur_dups',iset,dffull)

#find any non-identical duplicated Assessments still in data after patch
dupass=dffull.loc[dffull.duplicated(subset=['PIN','Inst'],keep=False)][['PIN','Assessment Name','Inst']]
dupass=dupass.loc[~(dupass.Inst.str.upper().str.contains('ASSESSMENT'))]
#booyah


#TURN THIS INTO A TICKET
duptix=dupass.drop_duplicates(subset='PIN').copy()
duptix['code']='ORANGE'
duptix['issueCode'] = 'AE5001'
duptix['datatype'] = 'TLBX'
duptix['reason'] = "Non-Identical Duplicate Found for "+duptix.Inst
i3=inventoryaabc3[['subject', 'study_id', 'redcap_event_name', 'redcap_event','event_date']].copy()
i3["PIN"]=i3.subject+'_'+i3.redcap_event
duptix=pd.merge(duptix,i3,on='PIN',how='left')

#'subject', 'study_id', 'redcap_event_name', 'redcap_event',
#       'event_date', 'PIN',

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


#DATE FORMAT IS STILL FUNKY ON THIS CHECK, better to examine by hand until can figure out why str.split isn't working.
#identical dups are removed if they have identical dates in original ssh command.  These will catch leftovers
#find cases where PIN was reused (e.g. PIN is the same but date more than 3 weeks different
dffull['Date']=dffull.DateFinished.str.split(' ', expand=True)[0]
catchdups=dffull.loc[~(dffull.Date=='')]
c=catchdups.drop_duplicates(subset=['PIN','Date'])[['PIN','Date']]
c.loc[c.duplicated(subset='PIN')]
#c is wierd.

#add subject and visit
df2=dffull.drop_duplicates(subset='PIN').copy()
df2['redcap_event']=df2.PIN.str.split("_",expand=True)[1]
df2['subject']=df2.PIN.str.split("_",expand=True)[0]
df2['redcap_event']=df2.PIN.str.split("_",expand=True)[1]
df2['TLBX']='YES'

#now merge with inventory
inventoryaabc4=pd.merge(inventoryaabc3,df2[['subject','redcap_event','TLBX','PIN']],on=['subject','redcap_event'],how='outer',indicator=True)

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
    print(inventoryaabc4.loc[inventoryaabc4._merge=='right_only'][['PIN','subject','redcap_event']])

inventoryaabc4=inventoryaabc4.loc[inventoryaabc4._merge!='right_only'].drop(columns=['_merge'])
#inventoryaabc2.to_csv('test.csv',index=False)

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

#T=pd.concat([duptix,t1,t2,t3])[['subject','study_id','redcap_event_name','redcap_event', 'event_date','PIN', 'reason', 'code','issueCode','datatype']]
T=pd.concat([duptix,t1,t2,t3])[['subject','study_id','redcap_event_name','redcap_event', 'event_date','PIN', 'reason', 'code','issueCode','datatype']]
#merge with site num from inventory
T=pd.merge(T,inventoryaabc4[['subject','redcap_event','site']],on=['subject','redcap_event'],how='left')

### NOW For ASA 24 ######################################################################
# ORDER
# 1. scan for data (here just looking for existende)
# 2. QC ids (after incorporating patches and translating ASAID into AABC id)
# 3. generate tickets and send to JIra if don't already exist
#  4. send tickets that arent identical to ones already in Jira
# # # 5. just dump all legit data to BOX (transform to be defined later) after patching, dropping restricted variables, and merging in subject and redcap_event
# # # 6. create and send snapshot of patched data to BOX after dropping restricted variables

folderqueue=['WU','MGH','UMN']  #, UCLA and MGH not started yet
anydata=[]
for studyshort in folderqueue:
    folder = config['NonQBox']['ASA24'][studyshort]
    dag = config['Redcap']['datasources']['aabcarms'][studyshort]['dag']
    sitenum = config['Redcap']['datasources']['aabcarms'][studyshort]['sitenum']
    filelist=box.list_of_files([folder])
    db=pd.DataFrame(filelist).transpose()#.reset_index().rename(columns={'index':'fileid'})
    dbitems=db.copy() #db.loc[db.filename.str.contains('TNS')].copy()
    subs=[]
    for f in dbitems.fileid:
        print(f)
        k=box.read_csv(f)
        if not k.empty:
            s=k.UserName.unique().tolist()
            subs=subs+s
    anydata=anydata+list(set(subs))

AD=pd.DataFrame(anydata,columns=['asa24id'])
AD['ASA24']='YES'
inventoryaabc5=pd.merge(inventoryaabc4,AD,on='asa24id',how='left')
missingAD=inventoryaabc5.loc[(inventoryaabc5.redcap_event_name.str.contains('v')) & (~(inventoryaabc5.ASA24=='YES'))]
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

#################################################################################
#ACTIGRAPHY
### for now, this is basically the same protocol as for ASA24
# TO DO Code for typos within the file (e.g. would be typos found in subsequent exports of database)
#scan BOX
folderqueue=['WU','UMN','MGH']#,'UCLA']
actdata=[]
studyshort='WU'
for studyshort in folderqueue:
    print(studyshort)
    folder = config['NonQBox']['Actigraphy'][studyshort]
    dag = config['Redcap']['datasources']['aabcarms'][studyshort]['dag']
    sitenum = config['Redcap']['datasources']['aabcarms'][studyshort]['sitenum']
    filelist=box.list_of_files([folder])
    db=pd.DataFrame(filelist).transpose()#.reset_index().rename(columns={'index':'fileid'})
    dbitems=db.copy() #db.loc[db.filename.str.contains('TNS')].copy()
    actsubs=[]
    for fid in dbitems.fileid:
        try:
            patrn = 'Identity'
            f=box.downloadFile(fid, download_dir="tmp", override_if_exists=False)
            print(f)
            file_one = open(f, "r")
            variable = file_one.readline(1)
            if not variable=='':
                for l in file_one.readlines():
                    if re.search(patrn, l):
                        hcaid=''
                        hcaid=l.strip("\n").replace('"', '').split(',')[1]
                        print("Inner",f,"has",hcaid)
                        actsubs=actsubs+[hcaid]
            file_one.close()
        except:
            print("Something the matter with file",f)
    actdata=actdata+list(actsubs)#list(set(actsubs))

#Duplicates?
if [item for item, count in collections.Counter(actdata).items() if count > 1] != '':
    print('Duplicated Actigraphy Record Found:',[item for item, count in collections.Counter(actdata).items() if count > 1])

ActD=pd.DataFrame(actdata,columns=['PIN'])
ActD['Actigraphy']='YES'
inventoryaabc6=pd.merge(inventoryaabc5,ActD,on='PIN',how='left')

#Missing?
missingAct=inventoryaabc6.loc[(inventoryaabc6.redcap_event_name.str.contains('v')) & (~(inventoryaabc6.Actigraphy=='YES'))]
missingAct=missingAct.loc[(missingAct.actigraphy_collectyn!='0') & (missingAct.actigraphy_partial_1 !=0)]
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



#MOCA SPANISH  #############################################################
## no data yet

############################################################################
# Psychopy
# ORDER
# 1. scan for data in Box (here just looking for existence) and in IntraDB
# 2. QC ids (after incorporating patches)
# 3. generate an tickets and send to JIra if don't already exist
# 4. DONT dump or snapshot.  Leave data in IntraDB.

studyshort='WU'
folderqueue=['WU','MGH','UMN','UCLA']

#scan Box
anydata=pd.DataFrame()
for studyshort in folderqueue:
    folder = config['NonQBox']['Psychopy'][studyshort]
    #dag = config['Redcap']['datasources']['aabcarms'][studyshort]['dag']
    #sitenum = config['Redcap']['datasources']['aabcarms'][studyshort]['sitenum']
    filelist=box.list_of_files([folder])
    db=pd.DataFrame(filelist).transpose()#.reset_index().rename(columns={'index':'fileid'})
    dbitems=db.copy() #db.loc[db.filename.str.contains('TNS')].copy()
    subs=[]
    for fname in dbitems.filename:
        #print(f)
        subjvscan = fname[fname.find('HCA'):fname.find('HCA')+15]
        l2=subjvscan.split('_')
        row=l2+[fname]
        print(row)
        rowfor=pd.DataFrame(row).transpose()
        anydata=pd.concat([anydata,rowfor])

anydata.columns=['subject','redcap_event','scan','fname']
PSY=anydata[['subject','redcap_event']].drop_duplicates()
checkIDB=anydata[['subject','redcap_event','scan']]
checkIDB['PIN_AB']=checkIDB.subject+'_'+checkIDB.redcap_event + '_'+checkIDB.scan
ci=checkIDB.drop_duplicates(subset='PIN_AB')

#just check for existence of PsychoPY in IntraDB
#/ceph/intradb/archive/AABC_WU_ITK/arc001/HCA7281271_V3_B/RESOURCES/LINKED_DATA/PSYCHOPY/
psychointradb4 = run_ssh_cmd('plenzini@login3.chpc.wustl.edu',
                       "ls /ceph/intradb/archive/AABC_WU_ITK/arc001/*/RESOURCES/LINKED_DATA/PSYCHOPY/ | cut -d'_' -f2,3,4 | grep HCA | grep -E -v 'ITK|Eye|tt' | sort -u").stdout.read()
psychointradb3 = run_ssh_cmd('plenzini@login3.chpc.wustl.edu',
                       "ls /ceph/intradb/archive/AABC_UMN_ITK/arc001/*/RESOURCES/LINKED_DATA/PSYCHOPY/ | cut -d'_' -f2,3,4 | grep HCA | grep -E -v 'ITK|Eye|tt' | sort -u").stdout.read()
psychointradb2 = run_ssh_cmd('plenzini@login3.chpc.wustl.edu',
                       "ls /ceph/intradb/archive/AABC_UCLA_ITK/arc001/*/RESOURCES/LINKED_DATA/PSYCHOPY/ | cut -d'_' -f2,3,4 | grep HCA | grep -E -v 'ITK|Eye|tt' | sort -u").stdout.read()
psychointradb1 = run_ssh_cmd('plenzini@login3.chpc.wustl.edu',
                       "ls /ceph/intradb/archive/AABC_MGH_ITK/arc001/*/RESOURCES/LINKED_DATA/PSYCHOPY/ | cut -d'_' -f2,3,4 | grep HCA | grep -E -v 'ITK|Eye|tt' | sort -u").stdout.read()

df4 = pd.DataFrame(str.splitlines(psychointradb4.decode('utf-8')))
df4 = df4[0].str.split(',', expand=True)
df3 = pd.DataFrame(str.splitlines(psychointradb3.decode('utf-8')))
df3 = df3[0].str.split(',', expand=True)
#df2 = pd.DataFrame(str.splitlines(psychointradb2.decode('utf-8')))
#df2 = df2[0].str.split(',', expand=True)
df1 = pd.DataFrame(str.splitlines(psychointradb1.decode('utf-8')))
df1 = df1[0].str.split(',', expand=True)

df=pd.concat([df1,df3,df4],axis=0) #df2,
df.columns = ['PIN_AB']
df.PIN_AB=df.PIN_AB.str.replace('t','')

#merge df (intradb) and ci (Box) to see what's missing - one of these is redundant with the check against AABC...
psymiss=pd.merge(ci,df, on='PIN_AB',how='outer',indicator=True)
#p1=pd.DataFrame()

print('A and or B psychopy in BOX but not in IntraDB')
for i in psymiss.loc[psymiss._merge=='left_only'].PIN_AB.unique():
    if i.split(sep='_')[2]=='A' or i.split(sep='_')[2]=='B':
        print(i)
p1=pd.DataFrame()
if psymiss.loc[psymiss._merge=='left_only'].shape[0]>0:
    p1 = pd.DataFrame(psymiss.loc[psymiss._merge=='left_only'].PIN_AB.unique())
    newp1=pd.DataFrame([item for item in list(p1[0]) if ('_A' in item or '_B' in item)])
    newp1['code']='ORANGE'
    newp1['issueCode']='AE4001'
    newp1['datatype']='PsychoPy'
    newp1['reason']='A and/or B Psychopy Data Found in Box but not IntraDB'

#p2=pd.DataFrame()

print('psychopy in IntraDB but not in Box')
for i in psymiss.loc[psymiss._merge=='right_only'].PIN_AB.unique():
    print(i)

p2=pd.DataFrame()
if psymiss.loc[psymiss._merge=='right_only'].shape[0] > 0:
    p2 = pd.DataFrame(psymiss.loc[psymiss._merge=='right_only'].PIN_AB.unique())
    p2['code']='ORANGE'
    p2['datatype']='PsychoPy'
    p2['issueCode']='AE4001'
    p2['reason']='Psychopy Data Found in IntraDB but not Box'

pwho=pd.DataFrame()
p=pd.concat([newp1,p2])
if p.shape[0]>0:#,columns='PIN_AB')
    p['PIN']=p[0].str[:13]
    p['PIN_AB']=p[0]
    p['subject_id']=p[0].str[:10]
    p['subject']=p[0].str[:10]
    pwho=pd.merge(inventoryaabc6.loc[inventoryaabc6.redcap_event.str.contains('V')].drop(columns=['subject_id','subject']),p,on='PIN',how='right')
    pwho=pwho[['subject','subject_id', 'study_id', 'redcap_event','redcap_event_name', 'site','reason','issueCode','code','v0_date','event_date','PIN_AB','datatype']]

#dont worry about duplicates in IntraDB - these will be filtered.
#find subjects in AABC but not in IntraDB or BOX
PSY2=psymiss.drop_duplicates(subset='subject')[['subject','redcap_event']]
PSY2['Psychopy']='YES'
inventoryaabc7=pd.merge(inventoryaabc6,PSY2,on=['subject','redcap_event'],how='left')
missingPY=inventoryaabc7.loc[(inventoryaabc7.redcap_event_name.str.contains('v')) & (~(inventoryaabc7.Psychopy=='YES'))].copy()
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

##################################################################################
#HOT FLASH DATA (not available yet)

###################################################################################
#VNS (not available yet)

###################################################################################
# To DO: Forgot to CHECK FOR BUNK IDS IN PSYCHOPY AND ACTIGRAPHY
###################################################################################


###################################################################################
# NOW CHECK key REDCap AABC variables for completeness (counterbalance, inventory completeness, age, bmi and soon to be more)
# inventory_complete
pd.set_option('display.width', 1000)
pd.options.display.width=1000

cb=inventoryaabc7.loc[(inventoryaabc7.redcap_event_name.str.contains('register')) & (inventoryaabc7.counterbalance_2nd=='')][['site','study_id','redcap_event','redcap_event_name','subject','v0_date','passedscreen']]
if cb.shape[0]>0:
    print("Current Counterbalance Ratio:\n", inventoryaabc7.counterbalance_2nd.value_counts())
    print("Currently Missing Counterbalance:", cb)
    cb['reason']='Currently Missing Counterbalance'
    cb['code']='RED'
    cb['datatype']='REDCap'
    cb['issueCode']='AE3001'
#QC
C=cb[['subject','redcap_event','study_id', 'site','reason','code','issueCode','v0_date','datatype']]
C.rename(columns={'v0_date':'event_date'})


summv=inventoryaabc7.loc[inventoryaabc7.redcap_event_name.str.contains('v')][['study_id','site','subject','redcap_event','visit_summary_complete','event_date']]
summv=summv.loc[~(summv.visit_summary_complete=='2')]
if summv.shape[0]>0:
    summv['code']='GREEN'
    summv['issueCode']='AE2001'
    summv['datatype']='REDCap'
    summv['reason']='Visit Summary Incomplete'
    summv=summv[['subject','redcap_event','study_id', 'site','reason','code','issueCode','event_date','datatype']]
    print("Visit Summary Incomplete:\n",summv)

agev=inventoryaabc7.loc[inventoryaabc7.redcap_event_name.str.contains('v')][['redcap_event', 'study_id', 'site','subject','redcap_event_name','age_visit','event_date','v0_date']]
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
#inventoryaabc7.loc[inventoryaabc7.redcap_event_name.str.contains('v')][['subject','redcap_event_name','height_ft','height_in','weight','bmi','event_date']]
bmiv=inventoryaabc7.loc[inventoryaabc7.redcap_event_name.str.contains('v')][['bmi','redcap_event','subject','study_id', 'site','event_date']].copy()
#outliers
#add in check against the extreme value confirmation and then relax the extremes
a=bmiv.loc[bmiv.bmi !=''].copy()
a=a.loc[(a.bmi.astype('float')<=17.0) | (a.bmi.astype('float')>=41)].copy()
if a.shape[0]>0:
    print("BMI OUTLIERS:\n", a.loc[(a.bmi.astype('float') <= 17) | (a.bmi.astype('float') >= 41)])
    a['code']='RED'
    a['datatype']='REDCap'
    a['reason']='BMI is an outlier.  Please double check height and weight'
    a['issueCode']='AE7001'
    a=a[['subject','redcap_event','study_id', 'site','reason','code','issueCode','event_date','datatype']]


#missings
bmiv=bmiv.loc[bmiv.bmi=='']
if bmiv.shape[0]>0:
    bmiv['code']='RED'
    bmiv['issueCode']='AE3001'
    bmiv['datatype']='REDCap'
    bmiv['reason']='Missing Height or Weight (or there is another typo preventing BMI calculation)'
    #bmiv=bmiv.loc[(bmiv.age_visit.astype(float).isnull()==True)]
    print("Missing BMI:\n",bmiv.loc[bmiv.bmi==''])
    bmiv=bmiv[['subject','redcap_event','study_id', 'site','reason','code','issueCode','event_date','datatype']]


##############################################################################
#all the flags for JIRA together
QAAP=concat(Q1,Q2,a1,a2,P,C,summv,agemv,ageav,a,bmiv,T)
QAAP['QCdate'] = date.today().strftime("%Y-%m-%d")
QAAP['issue_age']=(pd.to_datetime(QAAP.QCdate) - pd.to_datetime(QAAP.event_date))
QAAP=QAAP[['subject','redcap_event','study_id', 'site','reason','code','issueCode','event_date','issue_age','datatype']]
QAAP.sort_values(['site','issue_age'],ascending=False).to_csv('All_Issues_'+date.today().strftime("%d%b%Y")+'.csv',index=False)


###REDUCE by Color code.... need to be able to change these values.
filteredQ=QAAP.loc[((QAAP.code=='RED') & (QAAP.issue_age.dt.days>7)) | ((QAAP.code=='RED') & (QAAP.issue_age.dt.days.isnull()==True)) |  ((QAAP.code=='ORANGE') & (QAAP.issue_age.dt.days>18)) |  ((QAAP.code=='YELLOW') & (QAAP.issue_age.dt.days>28)) |  ((QAAP.code=='GREEN') & (QAAP.issue_age.dt.days>35)) ]
filteredQ.to_csv('FilteredQC4Jira.csv',index=False)
##RED=issue_age>7
##ORANGE=issues_age>18
##YELLOW=issue_age>28
##GREEN=issue_age>35

####### Download existing JIRA tickets and reduce QAAP accordingly
## create and upload new tickets.
#


########################################################################
#clean up the AABC inventory and upload to BOX for recruitment stats.
inventoryaabc7.loc[inventoryaabc7.age=='','age']=inventoryaabc6.age_visit
inventoryaabc7.loc[inventoryaabc7.event_date=='','event_date']=inventoryaabc7.v0_date
inventoryaabc7=inventoryaabc7.sort_values(['redcap_event','event_date'])
inventoryaabc7[['study_id','redcap_event_name','redcap_event','subject', 'site',
       'age', 'sex', 'event_date',
       'passedscreen', 'counterbalance_1st',
       'Qint','ravlt_collectyn', 'TLBX','nih_toolbox_collectyn', 'nih_toolbox_upload_typo',
                'ASA24','asa24yn','asa24id',
                'Actigraphy','actigraphy_collectyn', 'vms_collectyn',
                'legacy_yn', 'psuedo_guid', 'ethnic', 'racial',
       'visit_summary_complete']].to_csv('Inventory_Beta.csv',index=False)

# TO DO
#HARMONIZE Event Names
#Add filenames to TLBX data
#SEND INVENTORY, REDCAPs, and TLBX to PreRelease BOX
#HCA drop confusing variables
#inventoryaabc5.to_csv('Inventory_Beta.csv',index=False)
##upload



#    Check IntraDB for file and IDs therein - read and apply patch
# Actigraphy
#    Check IntraDB for file and IDs therein - read and apply patch
# VNS
#   Check IntraDB for file and IDs therein - read and apply patch
# ASR Spanish
#    Searcj for spanish language ASR, and upload subset of questions to REDCap.
#    Check that this has been done
# Moca
#    Look for missing MOCA, check for file, and ping RA to upload.







