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

config = LoadSettings()
#filename="./config.yml"
#with open(filename, 'r') as fd:
#        config=yaml.load(fd, Loader=yaml.SafeLoader)
secret=pd.read_csv(config['config_files']['secrets'])

box = LifespanBox(cache="./tmp")
pathp=box.downloadFile(config['hcainventory'])
ids=pd.read_csv(pathp)
hcaids=ids.subject.drop_duplicates()
hca_lastvisits=ids[['subject','redcap_event']].loc[ids.redcap_event.isin(['V1','V2'])].sort_values('redcap_event').drop_duplicates(subset='subject',keep='last')


#CREATE TIME DEPENDENT FLAGS
pd.concat([x for x in args if not x.empty])
#
#########################################################################################
#PHASE 0 TEST IDS AND ARMS
# if Legacy, id exists in HCA and other subject id related tests:
#Test that #visits in HCA corresponds with cohort in AABC
#i.e. if V2 exists, then cohort=
aabcarms = redjson(tok=secret.loc[secret.source=='aabcarms','api_key'].reset_index().drop(columns='index').api_key[0])
hcpa = redjson(tok=secret.loc[secret.source=='hcpa','api_key'].reset_index().drop(columns='index').api_key[0])
aabcreport = redreport(tok=secret.loc[secret.source=='aabcarms','api_key'].reset_index().drop(columns='index').api_key[0],reportid='51031')

#aabcarmsdf=getframe(struct=aabcarms,api_url=config['Redcap']['api_url10'])
aabcinvent=getframe(struct=aabcreport,api_url=config['Redcap']['api_url10'])

study_id=config['Redcap']['datasources']['aabcarms']['redcapidvar']
#slim=aabcarmsdf[['study_id','redcap_event_name',study_id,'legacy_yn']].loc[(aabcarmsdf.redcap_event_name.str.contains('register'))]# & (~(aabcarmsdf[study_id].str.upper().str.contains('TEST')))]
slim=aabcinvent[['study_id','redcap_event_name',study_id,'legacy_yn','site']].loc[(aabcinvent.redcap_event_name.str.contains('register'))]# & (~(aabcinvent[study_id].str.upper().str.contains('TEST')))]

fortest=pd.merge(hcaids,slim,left_on='subject',right_on=study_id,how="outer",indicator=True)
fortest._merge.value_counts()
legacyarms=['register_arm_1','register_arm_2','register_arm_3','register_arm_4','register_arm_5','register_arm_6','register_arm_7','register_arm_8']

#send these to Angela for emergency correction:
ft=fortest.loc[(fortest._merge=='right_only') & ((fortest.legacy_yn=='1')|(fortest.redcap_event_name.isin(legacyarms)))]
ft=ft.loc[~((ft[study_id]=='')|(ft[study_id].str.upper().str.contains('TEST')))]
qlist1=pd.DataFrame()
if not ft.empty:
    ft['reason']='Subject found in AABC REDCap Database with legacy indications whose ID was not found in HCP-A list'
    ft['code']='RED'
    qlist1=ft[['subject_id', 'study_id', 'redcap_event_name', 'site','reason','code','v0_date']]
    for s in list(ft[study_id].unique()):
        print('CODE RED :',s,': Subject found in AABC REDCap Database with legacy indications whose ID was not found in HCP-A list')

#if legacy v1 and enrolled as if v3 or v4 or legacy v2 and enrolled v4
ft2=fortest.loc[(fortest._merge=='both') & ((fortest.legacy_yn != '1')|(~(fortest.redcap_event_name.isin(legacyarms))))]
qlist2=pd.DataFrame()
if not ft2.empty:
    ft2['reason']='Subject found in AABC REDCap Database with legacy indications whose ID was not found in HCP-A list'
    ft2['code']='RED'
    qlist2 = ft2[['subject_id', 'study_id', 'redcap_event_name', 'site','reason','code','v0_date']]
    for s2 in list(ft2[study_id].unique()):
        print('CODE RED :',s2,': Subject found in AABC REDCap Database with an ID from HCP-A study but no legacyYN not checked')

#if legacy v1 and enrolled as if v3 or v4 or legacy v2 and enrolled v4
#get last visit
hca_lastvisits["next_visit"]=''
aabcidvisits=idvisits(aabcinvent,keepsies=['study_id','redcap_event_name','site','subject_id','site','v0_date','event_date'])
sortaabc=aabcidvisits.sort_values(['study_id','redcap_event_name'])
sortaabcv=sortaabc.loc[~(sortaabc.redcap_event_name.str.contains('register'))]
sortaabcv.drop_duplicates(subset=['study_id'],keep='first')
hca_lastvisits.next_visit=hca_lastvisits.redcap_event.str.replace('V','').astype('int') +1
hca_lastvisits["next_visit2"]="V"+hca_lastvisits.next_visit.astype(str)
hca_lastvisits2=hca_lastvisits.drop(columns=['redcap_event','next_visit'])
check=pd.merge(hca_lastvisits2,sortaabcv,left_on=['subject','next_visit2'],right_on=['subject','redcap_event'],how='outer',indicator=True)
check=check.loc[check._merge !='left_only']
wrongvisit=check.loc[check._merge=='right_only']
wrongvisit=wrongvisit.loc[~(wrongvisit.redcap_event=='phone_call_arm_13')]
qlist3=pd.DataFrame()
if not wrongvisit.empty:
    wrongvisit['reason']='Subject found in AABC REDCap Database initiating the wrong visit sequence (e.g. V3 insteady of V2'
    wrongvisit['code']='RED'
    qlist3 = wrongvisit[['subject_id', 'study_id', 'redcap_event_name', 'site','reason','code','event_date']]
    for s3 in list(wrongvisit[study_id].unique()):
        if s3 !='':
            print('CODE RED :',s3,': Subject found in AABC REDCap Database initiating the wrong visit sequence (e.g. V3 insteady of V2')

missingsubids=aabcinvent.loc[(aabcinvent.redcap_event_name.str.contains('register')) & (aabcinvent[study_id]=='')]
qlist4=pd.DataFrame()
if not missingsubids.empty:
    missingsubids['reason']='Subject ID is MISSING in AABC REDCap Database Record with study id'
    missingsubids['code']='ORANGE'
    qlist4 = missingsubids[['subject_id', 'study_id', 'redcap_event_name', 'site','reason','code','v0_date','event_date']]
    for s4 in list(missingsubids.study_id.unique()):
        print('CODE ORANGE : Subject ID is MISSING in AABC REDCap Database Record with study id:',s4)

#test subjects that need to be deleted
tests=aabcinvent.loc[(aabcinvent[study_id].str.upper().str.contains('TEST'))][['study_id',study_id,'redcap_event_name']]
qlist5=pd.DataFrame()
if not tests.empty:
    tests['reason']='HOUSEKEEPING : Please delete test subject.  Use test database when practicing'
    tests['code']='HOUSEKEEPING'
    qlist5 = tests[['subject_id', 'study_id', 'redcap_event_name', 'site','reason','code','v0_date','event_date']]
    for s5 in list(tests[study_id].unique()):
        print('HOUSEKEEPING : Please consider deleting test Subject:', s5)


#concatenate flags for REDCap key variables
Q0=pd.DataFrame(columns=['subject_id', 'study_id', 'redcap_event_name', 'site','reason','code','v0_date','event_date'])
Q1=pd.concat([Q0,qlist1,qlist2,qlist3,qlist4,qlist5],axis=0)

#########################################################################################
# PHASE 1 Test that all dataypes expected are present
# Expected (inventory):
keeplist=['study_id','redcap_event_name','v0_date','dob','age','sex','legacy_yn','psuedo_guid',
          'ethnic','racial','site','passedscreen','subject_id','counterbalance_1st','counterbalance_2nd','height_ft', 'height_in', 'weight','bmi', 'height_outlier_jira', 'height_missing_jira',
          'age_visit','event_date','completion_mocayn','ravlt_collectyn','nih_toolbox_collectyn','nih_toolbox_upload_typo',
          'tlbxwin_dups___1', 'tlbxwin_dups___2','tlbxwin_dups___3','actigraphy_collectyn',
          'vms_collectyn','face_complete','visit_summary_complete','asa24yn','asa24id']
inventoryaabc=idvisits(aabcinvent,keepsies=keeplist)
#inventoryaabc['redcap_event'] = inventoryaabc.replace({'redcap_event_name':
#                                               config['Redcap']['datasources']['aabcarms']['eventmap']})['redcap_event_name'].copy()
inventoryaabc = inventoryaabc.loc[~(inventoryaabc.subject_id.str.upper().str.contains('TEST'))].copy()


# Observed:
# pull Q data from Box to qint REDCap, then query qint against AABC-Arms study ids and visit
#    All records EVER created will be included in REDCap.
#    duplications
#    typos will be set to unusable automatically
#    missing: look for potential records in REDCap, first.  Correct in REDCap Not BOX or it will lead to duplicate.
#    if dup, set one to unususable and explain

firstvarcols = ['id', 'redcap_data_access_group', 'site', 'subjectid', 'fileid',
                'filename', 'sha1', 'created', 'assessment', 'visit', 'form',
                'q_unusable', 'unusable_specify', 'common_complete', 'ravlt_two']

columnnames = ['ravlt_pea_ravlt_sd_tc', 'ravlt_delay_scaled', 'ravlt_delay_completion', 'ravlt_discontinue',
               'ravlt_reverse', 'ravlt_pea_ravlt_sd_trial_i_tc', 'ravlt_pea_ravlt_sd_trial_ii_tc',
               'ravlt_pea_ravlt_sd_trial_iii_tc', 'ravlt_pea_ravlt_sd_trial_iv_tc', 'ravlt_pea_ravlt_sd_trial_v_tc',
               'ravlt_pea_ravlt_sd_listb_tc', 'ravlt_pea_ravlt_sd_trial_vi_tc', 'ravlt_recall_correct_trial1',
               'ravlt_recall_correct_trial2', 'ravlt_recall_correct_trial3', 'ravlt_recall_correct_trial4',
               'ravlt_delay_recall_correct', 'ravlt_delay_recall_intrusion', 'ravlt_delay_total_intrusion',
               'ravlt_delay_total_repetitions']


#current Qint Redcap:
#qintdata=redjson(tok=secret.loc[secret.source=='qint','api_key'].reset_index().drop(columns='index').api_key[0])
qintreport = redreport(tok=secret.loc[secret.source=='qint','api_key'].reset_index().drop(columns='index').api_key[0],reportid='51037')
qintdf=getframe(struct=qintreport,api_url=config['Redcap']['api_url10'])
aabcinvent=getframe(struct=aabcreport,api_url=config['Redcap']['api_url10'])


#all box files
folderqueue=['WU','UMN']#,'MGH','UCLA']
######
###THIS WHOLE SECTION NEEDS TO BE CRON'D - e.g. scan for anything new and import it into Qinteractive - let patch in REDCap handle bad or duplicate data.
#import anything new by any definition (new name, new sha, new fileid).
for studyshort in folderqueue:
    folder = config['Redcap']['datasources']['qint']['BoxFolders'][studyshort]
    dag = config['Redcap']['datasources']['aabcarms'][studyshort]['dag']
    sitenum = config['Redcap']['datasources']['aabcarms'][studyshort]['sitenum']

    filelist=box.list_of_files([folder])
    db=pd.DataFrame(filelist).transpose()#.reset_index().rename(columns={'index':'fileid'})
    db.fileid=db.fileid.astype(int)

    #ones that already exist in q redcap
    cached_filelist=qintdf.copy()
    cached_filelist.fileid=cached_filelist.fileid.astype('Int64') #ph.asInt(cached_filelist, 'fileid')

    #find the new ones that need to be pulled in
    newfileids=pd.merge(db,cached_filelist.fileid,on='fileid',how='left',indicator=True)
    newfileids=newfileids.loc[newfileids._merge=='left_only'].drop(columns=['_merge'])
    db2go=db.loc[db.fileid.isin(list(newfileids.fileid))]
    if db2go.empty:
        print("NO NEW RECORDS TO ADD AT THIS TIME")
    if not db2go.empty:
        #initiate new ids
        s = cached_filelist.id.astype('Int64').max() + 1
        l=len(db2go)
        vect=[]
        for i in range(0,l):
            id=i+s
            vect=vect+[id]

        rows2push=pd.DataFrame(columns=firstvarcols+columnnames)
        for i in range(0,db2go.shape[0]):
            redid=vect[i]
            fid=db2go.iloc[i][['fileid']][0]
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
            if len(rows2push.subjectid) > 0:
                print("**************** Summary **********************")
                print(len(rows2push.subjectid),"rows to push:")
                print(list(rows2push.subjectid))

if not rows2push.empty:
  send_frame(dataframe=rows2push, tok=secret.loc[secret.source=='qint','api_key'].reset_index().drop(columns='index').api_key[0])
###END SECTION THAT NEEDS TO BE TURNED INTO A CRON JOB

#QC checks
#now check
qintdf2=getframe(struct=qintreport,api_url=config['Redcap']['api_url10'])
invq=qintdf2[['id', 'site', 'subjectid','visit']].copy()
invq['redcap_event']="V"+invq.visit
invq['Qint']='YES'
invq=invq.loc[~(invq.subjectid.str.upper().str.contains('TEST'))]
#Before merging, check for duplicates that haven't been given the 'unusable' flag
dups=qintdf.loc[qintdf.duplicated(subset=['subjectid','visit'])]
dups2=dups.loc[~(dups.q_unusable.isnull()==False)]  #or '', not sure
if dups2.shape[0]>0:
    print("Duplicate Q-interactive records")
    print(dups2[['subjectid','visit']])

inventoryaabc2=pd.merge(inventoryaabc,invq.rename(columns={'subjectid':'subject'}).drop(columns=['site']),on=['subject','redcap_event'],how='outer',indicator=True)

q1=pd.DataFrame()
if inventoryaabc2.loc[inventoryaabc2._merge=='right_only'].shape[0] > 0 :
    print("The following ID(s)/Visit(s) are not found in the main AABC-ARMS Redcap.  Please investigate")
    print(inventoryaabc2.loc[inventoryaabc2._merge=='right_only'][['subject','redcap_event']])
    q1=inventoryaabc2.loc[inventoryaabc2._merge=='right_only'][['subject','redcap_event']]
    q1['reason']=['Subject with Q-int data but ID(s)/Visit(s) are not found in the main AABC-ARMS Redcap.  Please look for typo']
    q1['code']='ORANGE'

inventoryaabc2._merge.value_counts()
inventoryaabc3=inventoryaabc2.loc[inventoryaabc2._merge!='right_only'].drop(columns=['_merge'])
#inventoryaabc2.to_csv('test.csv',index=False)

missingQ=inventoryaabc3.loc[(inventoryaabc2.redcap_event_name.str.contains('v')) & (~(inventoryaabc2.Qint=='YES'))][['subject_id','study_id','subject','redcap_event','site','event_date']]
q2=pd.DataFrame()
if missingQ.shape[0]>0:
    print("Q-interactive cannot be found for")
    print(missingQ)
    q2=missingQ.copy()
    q2['reason']='Unable to locate Q-interactive data for this subject/visit'
    q2['code']='ORANGE'

Q0=pd.DataFrame(columns=['subject_id', 'study_id', 'redcap_event_name', 'site','reason','code','v0_date','event_date'])
Q2=pd.concat([Q0,q1,q2],axis=0)

# Toolbox  ###need to rename these all so that numbering 1::4 matches site convention
#note that you'll need to be on VPN for this to work
##FIRST THE RAW DATA FILES
rawd4 = run_ssh_cmd('plenzini@login3.chpc.wustl.edu',
                    'find /ceph/intradb/archive/AABC_WU_ITK/resources/toolbox_endpoint_data/ -type f  ! \( -name "*Scores*" -o -name "*Narrow*" -o -name "*Regist*" -o -name "*catalog*" \) -exec cat {} \;').stdout.read()
rawd1 = run_ssh_cmd('plenzini@login3.chpc.wustl.edu',
                    'find /ceph/intradb/archive/AABC_MGH_ITK/resources/toolbox_endpoint_data/ -type f  ! \( -name "*Scores*" -o -name "*Narrow*" -o -name "*Regist*" -o -name "*catalog*" \) -exec cat {} \;').stdout.read()
rawd3 = run_ssh_cmd('plenzini@login3.chpc.wustl.edu',
                    'find /ceph/intradb/archive/AABC_UMN_ITK/resources/toolbox_endpoint_data/ -type f  ! \( -name "*Scores*" -o -name "*Narrow*" -o -name "*Regist*" -o -name "*catalog*" \) -exec cat {} \;').stdout.read()
rawd2 = run_ssh_cmd('plenzini@login3.chpc.wustl.edu',
                    'find /ceph/intradb/archive/AABC_UCLA_ITK/resources/toolbox_endpoint_data/ -type f  ! \( -name "*Scores*" -o -name "*Narrow*" -o -name "*Regist*" -o -name "*catalog*" \) -exec cat {} \;').stdout.read()
raw41=TLBXreshape(rawd4)
#raw11=TLBXreshape(rawd2)
raw31=TLBXreshape(rawd3)
#raw21=TLBXreshape(rawd4)

rf2=pd.concat([raw11,raw31])
rf2=rf2.loc[~(rf2.PIN.str.upper().str.contains('TEST'))]
rf2=rf2.loc[~(rf2.PIN.str.upper()=='ABC123')]
rf2=rf2.drop_duplicates(subset='PIN').copy()

#fixtypos - need to incorporate information about date of session as given in filename because of typos involving legit ids
fixtypos=inventoryaabc2.loc[inventoryaabc2.nih_toolbox_upload_typo!=''][['subject','redcap_event','nih_toolbox_upload_typo']]
fixtypos['PIN']=fixtypos.subject+'_'+fixtypos.redcap_event
fixes=dict(zip(fixtypos.nih_toolbox_upload_typo, fixtypos.PIN))
rf2.PIN=rf2.PIN.replace(fixes)

df2['redcap_event']=df2.PIN.str.split("_",expand=True)[1]
df2['subject']=df2.PIN.str.split("_",expand=True)[0]
df2['redcap_event']=df2.PIN.str.split("_",expand=True)[1]
df2['TLBX']='YES'


#NOW THE SCORED DATA
results1 = run_ssh_cmd('plenzini@login3.chpc.wustl.edu',
                       'cat /ceph/intradb/archive/AABC_WU_ITK/resources/toolbox_endpoint_data/*Scores* | cut -d"," -f1,2,3,4,10 | sort -u').stdout.read()
results2 = run_ssh_cmd('plenzini@login3.chpc.wustl.edu',
                       'cat /ceph/intradb/archive/AABC_MGH_ITK/resources/toolbox_endpoint_data/*Scores* | cut -d"," -f1,2,3,4,10 | sort -u').stdout.read()
results3 = run_ssh_cmd('plenzini@login3.chpc.wustl.edu',
                       'cat /ceph/intradb/archive/AABC_UMN_ITK/resources/toolbox_endpoint_data/*Scores* | cut -d"," -f1,2,3,4,10 | sort -u').stdout.read()
results4 = run_ssh_cmd('plenzini@login3.chpc.wustl.edu',
                       'cat /ceph/intradb/archive/AABC_UCLA_ITK/resources/toolbox_endpoint_data/*Scores* | cut -d"," -f1,2,3,4,10 | sort -u').stdout.read()


dffull1=TLBXreshape(results1)
#dffull2=TLBXreshape(results2)
dffull3=TLBXreshape(results3)
#dffull4=TLBXreshape(results4)

#Find any duplicated Assessments
dupass1=dffull1.loc[dffull1.duplicated(subset=['PIN','Inst'],keep=False)][['PIN','Assessment Name','Inst']]
dupass3=dffull3.loc[dffull1.duplicated(subset=['PIN','Inst'],keep=False)][['PIN','Assessment Name','Inst']]
#dupass2=dffull2.loc[dffull1.duplicated(subset=['PIN','Inst'],keep=False)][['PIN','Assessment Name','Inst']]
#dupass4=dffull4.loc[dffull1.duplicated(subset=['PIN','Inst'],keep=False)][['PIN','Assessment Name','Inst']]

# HCA8596099_V3 has 2 assessments for Words in Noise - add patch note"
print('WU duplicated assessments')
print(dupass1)

###CHANGE THIS SO THAT SPECIFIC INSTRUMENT TO DELETE IS IN A SINGLE VARIABLE, since 1 2 3 are not the only options.
#merge with patch fixes (i.e. delete duplicate specified in visit summary)
fixass=inventoryaabc2[['subject','redcap_event','tlbxwin_dups___1','tlbxwin_dups___2','tlbxwin_dups___3']].copy()
fixass['Assessment Name']=''
fixass['Inst']=''
fixass.loc[fixass.tlbxwin_dups___1=='1','Assessment Name']=1
fixass.loc[fixass.tlbxwin_dups___2=='1','Assessment Name']=2
fixass.loc[fixass.tlbxwin_dups___3=='1','Assessment Name']=3
fixass.loc[fixass.tlbxwin_dups___1=='1','Inst']='NIH Toolbox Words-In-Noise Test Age 6+ v2.1'
fixass.loc[fixass.tlbxwin_dups___2=='1','Inst']='NIH Toolbox Words-In-Noise Test Age 6+ v2.1'
fixass.loc[fixass.tlbxwin_dups___3=='1','Inst']='NIH Toolbox Words-In-Noise Test Age 6+ v2.1'
fixass['PIN']=fixass.subject+'_'+fixass.redcap_event
badass=fixass.loc[~(fixass['Inst']=='')]

dffull=pd.concat([dffull1, dffull3])#, dffull2, dffull4])
#still working on this logic:
#dffull=pd.merge(dffull,badass[['PIN','Inst','Assessment Name']],on=['PIN','Inst','Assessment Name'],how='outer',indicator=True)

#date split isn't woring here...won't lead to the catching of PINs that were reused by accident on subsequent visits
#fix and then rewrite the last chec
dffull['Date']=dffull.DateFinished.str.split(' ',expand=True)[[0]]
df=dffull[['PIN', 'DeviceID', 'Assessment Name', 'Date']].rename(columns={'Date': 'DateFinished'}).drop_duplicates(subset=['PIN'])

df2=df.copy() #pd.concat([df11,df31])
df2=df2.loc[~(df2.PIN.str.upper().str.contains('TEST'))]

#make it such that don't need to delete Date column so that accidental PIN re-use can be caught
df2=df2.drop(columns='DateFinished')
#df2=df2.loc[~(df2.DateFinished=='')]
df2=df2.loc[~(df2.PIN.str.upper()=='ABC123')]

##fixtypos - need to extend and incorporate information about date of session as given in filename because of typos involving legit ids
df2.PIN=df2.PIN.replace(fixes)

#Either scored or raw is missing in format expected:
formats=pd.merge(df2,rf2,how='outer',on='PIN',indicator=True)[['PIN','_merge']]
issues=formats.loc[~(formats._merge=='both')]
if issues.shape[0]>0:
    print("Raw or Scored data not found (make sure you didn't export Narrow format")
    print(issues[['PIN']])

#add subject and visit
df2['redcap_event']=df2.PIN.str.split("_",expand=True)[1]
df2['subject']=df2.PIN.str.split("_",expand=True)[0]
df2['redcap_event']=df2.PIN.str.split("_",expand=True)[1]
df2['TLBX']='YES'

#identical dups are removed if they have identical dates in original ssh command.  These will catch leftovers
# ...e.g. because id was reused at a different time
#pd.set_option('display.width', 1000)
#pd.options.display.width=1000
dupsT=df2.loc[df2.duplicated(subset=['PIN'])]
if dupsT.shape[0]>0:
    print("Duplicate TLBX records")
    print(dupsT[['PIN']])

#now merge with inventory
inventoryaabc4=pd.merge(inventoryaabc3,df2[['subject','redcap_event','TLBX','PIN']],on=['subject','redcap_event'],how='outer',indicator=True)

#find toolbox records that aren't in AABC - typos are one thing...legit ids are bad because don't know which one is right unless look at date, which is missing for cog comps
inventoryaabc4.loc[inventoryaabc4._merge=='right_only']
#use HCA8596099_V2 which was entered as HCA8596099_V3 which will be an issue at next visit as an example.


if inventoryaabc4.loc[inventoryaabc4._merge=='right_only'].shape[0] > 0 :
    print("The following TOOLBOX PINs are not found in the main AABC-ARMS Redcap.  Please investigate")
    print(inventoryaabc4.loc[inventoryaabc4._merge=='right_only'][['PIN','subject','redcap_event']])

inventoryaabc4=inventoryaabc4.loc[inventoryaabc4._merge!='right_only'].drop(columns=['_merge'])
#inventoryaabc2.to_csv('test.csv',index=False)

missingT=inventoryaabc4.loc[(inventoryaabc4.redcap_event_name.str.contains('v')) & (~(inventoryaabc4.TLBX=='YES'))]
if missingT.shape[0]>0:
    print("TLBX cannot be found for")
    print(missingT[['subject','redcap_event','site','event_date','nih_toolbox_collectyn']])


#ASA24
folderqueue=['WU']

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
    anydata=list(set(subs))

AD=pd.DataFrame(anydata,columns=['asa24id'])
AD['ASA24']='YES'
inventoryaabc5=pd.merge(inventoryaabc4,AD,on='asa24id',how='left')
missingAD=inventoryaabc5.loc[(inventoryaabc5.redcap_event_name.str.contains('v')) & (~(inventoryaabc5.ASA24=='YES'))]
missingAD=missingAD.loc[~(missingAD.asa24yn=='0')]
if missingAD.shape[0]>0:
    print("ASA24 cannot be found for")
    print(missingAD[['subject','redcap_event','site','event_date','asa24yn','asa24id']])

#ACTIGRAPHY
folderqueue=['WU']

#something wierd is happening with the requests library...this doesn't work under requests, but does work under re
#I guess they are different.


for studyshort in folderqueue:
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
    actdata=list(actsubs)#list(set(actsubs))

#Duplicates?
if [item for item, count in collections.Counter(actdata).items() if count > 1] != '':
    print('Duplicated Actigraphy Record Found:',[item for item, count in collections.Counter(actdata).items() if count > 1])

ActD=pd.DataFrame(actdata,columns=['PIN'])
ActD['Actigraphy']='YES'
inventoryaabc6=pd.merge(inventoryaabc,ActD,on='PIN',how='left')
#Missing?
missingAct=inventoryaabc6.loc[(inventoryaabc6.redcap_event_name.str.contains('v')) & (~(inventoryaabc6.Actigraphy=='YES'))]
missingAct=missingAct.loc[~(missingAct.actigraphy_collectyn=='0')]
if missingAct.shape[0]>0:
    print("Actigraphy cannot be found for")
    print(missingAct[['subject','redcap_event','site','event_date','actigraphy_collectyn']])

#MOCA SPANISH

#Psychopy - just a super high level scan for existence of id (typo catch)
studyshort='WU'
folderqueue=['WU']

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
#anydata['ab']=''
#for s in list(anydata.subject.unique()):
#    snydata=anydata.loc[anydata.subject==s]
#    for v in list(snydata.redcap_event.unique()):
#       # this doesnt work because there are duplicate subjects in snydata
#        ab=list(snydata.loc[snydata.redcap_event==v].scan.unique())
#        #rint(s,v,ab)
#        #nydata.loc[(anydata.subject==s) & (anydata.redcap_event==v),'ab']=ab

PSY=anydata[['subject','redcap_event']].drop_duplicates()
PSY['PsychopyBox']='YES'
checkIDB=anydata[['subject','redcap_event','scan']]
checkIDB['PIN_AB']=checkIDB.subject+'_'+checkIDB.redcap_event + '_'+checkIDB.scan
ci=checkIDB.drop_duplicates(subset='PIN_AB')

#just check for existence of PsychoPY in IntraDB
#/ceph/intradb/archive/AABC_WU_ITK/arc001/HCA7281271_V3_B/RESOURCES/LINKED_DATA/PSYCHOPY/
psychointradb = run_ssh_cmd('plenzini@login3.chpc.wustl.edu',
                       "ls /ceph/intradb/archive/AABC_WU_ITK/arc001/*/RESOURCES/LINKED_DATA/PSYCHOPY/ | cut -d'_' -f2,3,4 | grep HCA | grep -E -v 'ITK|Eye|tt' | sort -u").stdout.read()

df = pd.DataFrame(str.splitlines(psychointradb.decode('utf-8')))
df = df[0].str.split(',', expand=True)
df.columns = ['PIN_AB']

#merge df and ci to see what's missing
psymiss=pd.merge(ci,df, on='PIN_AB',how='outer',indicator=True)
print('psychopy in BOX but not in IntraDB')
for i in psymiss.loc[psymiss._merge=='left_only'].PIN_AB.unique():
    print(i)


# not necessary to check details.  Java class checs this
#(base) [plenzini@login01 ~]$ ll /ceph/intradb/archive/AABC_WU_ITK/arc001/HCA7281271_V3_B/RESOURCES/LINKED_DATA/PSYCHOPY/
#total 46
#-rw-r----- 1 nrg-svc-hcpi nrg-hcpi-fs  9544 Jul 22 09:40 mbPCASL_HCA7281271_V3_B_2022-07-22_090925.log
#-rw-r----- 1 nrg-svc-hcpi nrg-hcpi-fs    23 Jul 22 09:40 mbPCASL_HCA7281271_V3_BEyeCamFPS_Dist.csv
#-rw-r----- 1 nrg-svc-hcpi nrg-hcpi-fs 36061 Jul 22 09:40 REST_HCA7281271_V3_B_2022-07-22_082603.log
#-rw-r----- 1 nrg-svc-hcpi nrg-hcpi-fs    34 Jul 22 09:40 REST_HCA7281271_V3_BEyeCamFPS_Dist.csv


inventoryaabc7=pd.merge(inventoryaabc6,PSY,on=['subject','redcap_event'],how='left')
missingPY=inventoryaabc7.loc[(inventoryaabc7.redcap_event_name.str.contains('v')) & (~(inventoryaabc7.PsychopyBox=='YES'))]
#missingPY=missingPY.loc[~(missingPY.asa24yn=='0')]
if missingPY.shape[0]>0:
    print("PsyhoPy (BOX copy) cannot be found for")
    print(missingPY[['subject','redcap_event','site','event_date','PsychopyBox']])


#HOT FLASH DATA (not available yet)
#GO BACK AND CHECK FOR BUNK IDS IN PSYCHOPY AND ACTIGRAPHY

#IntraDB ID

# NOW CHECK inventory variables for completeness
# inventory_complete
pd.set_option('display.width', 1000)
pd.options.display.width=1000

cb=inventoryaabc7.loc[(inventoryaabc7.redcap_event_name.str.contains('register')) & (inventoryaabc7.counterbalance_2nd=='')][['redcap_event_name','subject','v0_date','passedscreen']]
print("Current Counterbalance Ratio:\n",inventoryaabc7.counterbalance_2nd.value_counts())
print("Currently Missing Counterbalance:",print(cb))

summv=inventoryaabc7.loc[inventoryaabc7.redcap_event_name.str.contains('v')][['subject','redcap_event_name','visit_summary_complete','event_date']]
summv=summv.loc[~(summv.visit_summary_complete=='2')]
print("Visit Summary Incomplete:\n",summv)

agev=inventoryaabc7.loc[inventoryaabc7.redcap_event_name.str.contains('v')][['subject','redcap_event_name','age_visit','event_date']]
ag=agev.loc[agev.age_visit !='']
print("AGE OUTLIERS:\n",ag.loc[(ag.age_visit.astype('float')<=40) | (ag.age_visit.astype('float')>=90 )])

agev=agev.loc[(agev.age_visit.astype(float).isnull()==True)]
print("Missing Age:\n",agev)

#calculate BMI: weight (lb) / [height (in)]2 x 703
#inventoryaabc7.loc[inventoryaabc7.redcap_event_name.str.contains('v')][['subject','redcap_event_name','height_ft','height_in','weight','bmi','event_date']]
bmiv=inventoryaabc7.loc[inventoryaabc7.redcap_event_name.str.contains('v')][['redcap_event_name','subject','bmi','event_date']]
#bmiv=bmiv.loc[(bmiv.age_visit.astype(float).isnull()==True)]
print("Missing BMI:\n",bmiv.loc[bmiv.bmi==''])

#outliers
a=bmiv.loc[bmiv.bmi !='']
print("BMI OUTLIERS:\n",a.loc[(a.bmi.astype('float')<=19) | (a.bmi.astype('float')>=37)])

# event_dates (would be caught by missing age)

#send to csv
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


#HARMONIZE Event Names
#Add filenames to TLBX data
#SEND INVENTORY, REDCAPs, and TLBX to PreRelease BOX
#HCA drop confusing variables

#QC Psychopy IntraDB

#inventoryaabc5.to_csv('Inventory_Beta.csv',index=False)




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







