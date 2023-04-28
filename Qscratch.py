import pandas as pd
import pandas as pd
import yaml
import ccf
from ccf.box import LifespanBox
import requests
import re
import collections
from functions import *
#import functions
from config import *
import subprocess
import os
import sys
from datetime import date

## get configuration files
config = LoadSettings()
secret=pd.read_csv(config['config_files']['secrets'])
box = LifespanBox(cache="./tmp")
scratch=205351313707

aabcreport = redreport(tok=secret.loc[secret.source=='aabcarms','api_key'].reset_index().drop(columns='index').api_key[0],reportid='51031')
aabcinvent = getframe(struct=aabcreport,api_url=config['Redcap']['api_url10'])
inventoryaabc=idvisits(aabcinvent,keepsies=list(aabcinvent.columns))
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
folderqueue=['WU','UCLA','WU','UMN','MGH'] ###,'UCLA']
######
###THIS WHOLE SECTION NEEDS TO BE CRON'D - e.g. scan for anything new and import it into Qinteractive - let patch in REDCap handle bad or duplicate data.
#this is currently taing too much time to iterate through box
#import anything new by any definition (new name, new sha, new fileid)
studyshortsum=0
all2push=pd.DataFrame()
for studyshort in folderqueue:
    print(studyshort,"......")
    folder = config['Redcap']['datasources']['qint']['BoxFolders'][studyshort]
    dag = config['Redcap']['datasources']['aabcarms'][studyshort]['dag']
    sitenum = config['Redcap']['datasources']['aabcarms'][studyshort]['sitenum']

    filelist=box.list_of_files([folder])
    #print(filelist)
    db=pd.DataFrame(filelist).transpose()#.reset_index().rename(columns={'index':'fileid'})
    #print(db)
    db.fileid=db.fileid.astype(int)
    #ones that already exist in q redcap
    cached_filelist=qintdf.copy()
    #cached_filelist.fileid=cached_filelist.fileid.astype('Int64') #ph.asInt(cached_filelist, 'fileid')
    cached_filelist.fileid=cached_filelist.fileid.str.strip().astype(float).astype('Int64')
    #find the new ones that need to be pulled in
    newfileids=pd.merge(db.fileid,cached_filelist.fileid,on='fileid',how='left',indicator=True)
    newfileids_temp=newfileids.copy()
    newfileids=newfileids.loc[newfileids._merge=='left_only'].drop(columns=['_merge'])
    db2go=db.loc[db.fileid.isin(list(newfileids.fileid))]
    if db2go.empty:
        print("NO NEW RECORDS from",studyshort,"TO ADD AT THIS TIME")
    if not db2go.empty:
        #initiate new ids
        s = cached_filelist.id.str.strip().astype(float).astype('Int64').max() + 1 + studyshortsum
        l=len(db2go)
        vect=[]
        for i in range(0,l):
            id=i+s
            vect=vect+[id]
        studyshortsum=studyshortsum+l
        rows2push=pd.DataFrame(columns=firstvarcols+columnnames)
        for i in range(0,db2go.shape[0]):
            try:
                redid=vect[i]
                fid=db2go.iloc[i][['fileid']][0]
                #print("fid",fid)
                t=box.getFileById(fid)
                created=t.get().created_at
                fname=db2go.iloc[i][['filename']][0]
                subjid = fname[fname.find('HCA'):10]
                fsha=db2go.iloc[i][['sha1']][0]
                #print(i)
                #print(db2go.iloc[i][['fileid']][0])
                #print(db2go.iloc[i][['filename']][0])
                #print(db2go.iloc[i][['sha1']][0])
                #print("subject id:",subjid)
                #print("Redcap id:",redid)
                content=box.read_text(fid)
                assessment='RAVLT'
                if 'RAVLT-Alternate Form C' in content:
                            form = 'Form C'
                if 'RAVLT-Alternate Form D' in content:
                            form = 'Form D'
                if fname.find('Form B')>0:
                            form= 'Form B'
                a = fname.replace("AV", "").find('V')
                visit=fname[a+1]
                row=parse_content(content)
                df = pd.DataFrame([row], columns=columnnames)
                firstvars = pd.DataFrame([[redid, dag, sitenum, subjid, fid, fname, fsha, created, assessment,
                                           visit, form, "", "", "", ""]], columns=firstvarcols)
                pushrow=pd.concat([firstvars,df],axis=1)
                rows2push=pd.concat([rows2push,pushrow],axis=0)
            except:
                print("something is wrong with this file")
                print(db2go.iloc[i])
                print("...exporting to scratch")
                db2go.iloc[i].to_csv("./tmp/Corrupted_"+db.iloc[i].filename,index=False)
                box.upload_file("./tmp/Corrupted_"+db.iloc[i].filename,scratch)

        if len(rows2push.subjectid) > 0:
            print("*************",studyshort,"  **********************")
            print(len(rows2push.subjectid),"rows to push:")
            print(list(rows2push.subjectid))
            print("***************************************************")
        all2push=pd.concat([all2push,rows2push]).drop_duplicates()

if not all2push.empty:
    print("**************** Summary All Sites **********************")
    print(len(all2push.subjectid), "Total rows to push across sites:")
    send_frame(dataframe=all2push, tok=secret.loc[secret.source=='qint','api_key'].reset_index().drop(columns='index').api_key[0])

####
###END SECTION THAT NEEDS TO BE TURNED INTO A CRON JOB
