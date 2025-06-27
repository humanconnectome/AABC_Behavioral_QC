import pandas as pd
import numpy as np
import yaml
import ccf
from ccf.box import LifespanBox
import requests
import re
import collections
import subprocess
import os
import sys
from datetime import date
from subprocess import Popen, PIPE
from config import *
box = LifespanBox(cache="./tmp")



## get configuration files
config = LoadSettings()

#functions
def redjson(tok):
    aabcarms = {
        'token': tok,
        'content': 'record',
        'format': 'json',
        'type': 'flat',
        'csvDelimiter': '',
        'rawOrLabel': 'raw',
        'rawOrLabelHeaders': 'raw',
        'exportCheckboxLabel': 'false',
        'exportSurveyFields': 'false',
        'exportDataAccessGroups': 'false',
        'returnFormat': 'json'}
    return aabcarms

def redreport(tok,reportid):
    aabcreport = {
        'token':tok,
        'content': 'report',
        'format': 'json',
        'report_id': reportid,
        'csvDelimiter': '',
        'rawOrLabel': 'raw',
        'rawOrLabelHeaders': 'raw',
        'exportCheckboxLabel': 'false',
        'returnFormat': 'json'
    }
    return aabcreport

def getframe(struct,api_url):
    r = requests.post(api_url,data=struct)
    print('HTTP Status: ' + str(r.status_code))
    a=r.json()
    HCAdf=pd.DataFrame(a)
    return HCAdf

def idvisits(aabcarmsdf,keepsies):
    idvisit=aabcarmsdf[keepsies].copy()
    registers=idvisit.loc[idvisit.redcap_event_name.str.contains('register')][['subject_id','study_id','site']]
    idvisit=pd.merge(registers,idvisit.drop(columns=['site']),on='study_id',how='right')
    idvisit=idvisit.rename(columns={'subject_id_x':'subject','subject_id_y':'subject_id'})
    idvisit['redcap_event']=idvisit.replace({'redcap_event_name':
                                           config['Redcap']['datasources']['aabcarms']['AABCeventmap']})['redcap_event_name']
    idvisit = idvisit.loc[~(idvisit.subject.astype(str).str.upper().str.contains('TEST'))]
    return idvisit

def rollforward(aabcarmsdf,variable,event_start):
    varinit=aabcarmsdf[['subject',variable,'redcap_event']]
    print(varinit.shape)
    varinit=varinit.loc[varinit.redcap_event==event_start].drop(columns=['redcap_event']).drop_duplicates()
    print(varinit.shape)
    print(varinit.head())
    return aabcarmsdf.drop(columns=[variable]).merge(varinit)

#rollforward(inventorysnapshot,'legacy_yn','AF0')

def concat(*args):
    return pd.concat([x for x in args if not x.empty],axis=0)

def parse_content(content):
    #section_headers = [
    #    'Subtest,,Raw score',
    #    'Subtest,,Scaled score',
    #    'Subtest,Type,Total',  # this not in aging or RAVLT
    #    'Subtest,,Completion Time (seconds)',
    #    'Subtest,Type,Yes/No',
    #    'Item,,Raw score'
    #]
    section_headers = [
        'Subtest,,Raw score',
        'Subtest,,Scaled score',
        'Subtest,,Completion Time (seconds)',
        'Subtest,Type,Yes/No',
        'Item,,Raw score'
    ]
    # Last section header is repeat data except for RAVLT
    if 'RAVLT' in content:
        section_headers.append('Scoring Type,,Scores')

    new_row = []
    capture_flag = False
    for row in content.splitlines():
        row = row.strip(' "')
        if row in section_headers:
            capture_flag = True
            #print(row)

        elif row == '':
            capture_flag = False

        elif capture_flag:
            value = row.split(',')[-1].strip()

            if value == '-':
                value = ''
            new_row.append(value)

    return new_row

def send_frame(dataframe, tok):
    data = {
        'token': tok,
        'content': 'record',
        'format': 'csv',
        'type': 'flat',
        'overwriteBehavior': 'normal',
        'forceAutoNumber': 'false',
        'data': dataframe.to_csv(index=False),
        'returnContent': 'ids',
        'returnFormat': 'json'
    }
    r = requests.post('https://redcap.wustl.edu/redcap/api/', data=data)
    print('HTTP Status: ' + str(r.status_code))
    print(r.json())


def run_ssh_cmd(host, cmd):
    cmds = ['ssh', '-t', '-i', '/Users/petralenzini/.ssh/plmacchpc',host, cmd]
    return Popen(cmds, stdout=PIPE, stderr=PIPE, stdin=PIPE)


def getlist(mask,sheet):
    restrictA=pd.read_excel(mask, sheet_name=sheet)
    restrictedA=list(restrictA.field_name)
    return restrictedA

def TLBXreshape(results1):
    #df=results1.decode('utf-8')
    df=pd.DataFrame(str.splitlines(results1.decode('utf-8')))
    df=df[0].str.split(',', expand=True)
    cols=df.loc[df[0]=='PIN'].values.tolist()
    df2=df.loc[~(df[0]=='PIN')]
    df2.columns=cols[0]
    return df2
#REPLACE PETRA VERSION WITH PRESTION VERSION
#def importTLBX(siteabbrev='WU',typed='scores'):
#    if typed=='scores':
#        run_ssh_cmd('plenzini@login3.chpc.wustl.edu',
#                    'find /ceph/intradb/archive/AABC_' + siteabbrev + '_ITK/resources/toolbox_endpoint_data/ -type f  -name "*Scores*" ! \( -name "*Narrow*" -o -name "*Regist*" -o -name "*catalog*" \) > /home/plenzini/tools/catTLBX/datalist.csv').stdout.read()
#    else:
#        run_ssh_cmd('plenzini@login3.chpc.wustl.edu',
#                        'find /ceph/intradb/archive/AABC_'+siteabbrev+'_ITK/resources/toolbox_endpoint_data/ -type f  ! \( -name "*Scores*" -o -name "*Narrow*" -o -name "*Regist*" -o -name "*catalog*" \) > /home/plenzini/tools/catTLBX/datalist.csv').stdout.read()
#    run_ssh_cmd('plenzini@login3.chpc.wustl.edu',
#                'cd /home/plenzini/tools/catTLBX/cache; while read i; do cp "$i" .; done < /home/plenzini/tools/catTLBX/datalist.csv').stdout.read()
#    #run_ssh_cmd('plenzini@login3.chpc.wustl.edu',
#    #                    'while read i; do cp "$i" /home/plenzini/tools/catTLBX/cache/.; done < /home/plenzini/tools/catTLBX/datalist.csv').stdout.read()
#    run_ssh_cmd('plenzini@login3.chpc.wustl.edu',
#                'for f in /home/plenzini/tools/catTLBX/cache/*\ *; do mv "$f" "${f// /_}"; done').stdout.read()
#    run_ssh_cmd('plenzini@login3.chpc.wustl.edu',
#                'find /home/plenzini/tools/catTLBX/cache/ -type f > /home/plenzini/tools/catTLBX/datalist2.csv').stdout.read()
#    run_ssh_cmd('plenzini@login3.chpc.wustl.edu',
#                "sed -i 's/\/home\/plenzini/\/Users\/petralenzini\/chpc3/g' /home/plenzini/tools/catTLBX/datalist2.csv").stdout.read()
#    # Using readlines()
#    file1 = open('/Users/petralenzini/chpc3/tools/catTLBX/datalist2.csv', 'r')
#    Lines = file1.readlines()
#    sitedf=pd.DataFrame()
#    count = 0
#    # Strips the newline character
#    for line in Lines:
#        count += 1
#        print(count)
#        subsetdf=pd.read_csv(line.strip("\n"))
#        sitedf=pd.concat([sitedf,subsetdf],axis=0)
#        sitedf['sitestr']=siteabbrev
#    file1.close()
#    run_ssh_cmd('plenzini@login3.chpc.wustl.edu',
#                "rm -f /home/plenzini/tools/catTLBX/cache/* /home/plenzini/tools/catTLBX/datalist* ").stdout.read()
#    return sitedf


def importTLBX(siteabbrev='WU',typed='scores',username='plenzini@login3.chpc.wustl.edu'):
    if typed=='scores': 
        run_ssh_cmd(username,
                    'find /ceph/intradb/archive/AABC_' + siteabbrev + '_ITK/resources/toolbox_endpoint_data/ -type f  -name "*Scores*" ! \( -name "*Narrow*" -o -name "*Regist*" -o -name "*catalog*" \) > /home/plenzini/tools/catTLBX/datalist.csv').stdout.read()
    else:
        run_ssh_cmd(username,
                        'find /ceph/intradb/archive/AABC_'+siteabbrev+'_ITK/resources/toolbox_endpoint_data/ -type f  ! \( -name "*Scores*" -o -name "*Narrow*" -o -name "*Regist*" -o -name "*catalog*" \) > /home/plenzini/tools/catTLBX/datalist.csv').stdout.read()
    run_ssh_cmd(username,
                'cd /home/plenzini/tools/catTLBX/cache; while read i; do cp "$i" .; done < /home/plenzini/tools/catTLBX/datalist.csv').stdout.read()
    #run_ssh_cmd('plenzini@login3.chpc.wustl.edu',
    #                    'while read i; do cp "$i" /home/plenzini/tools/catTLBX/cache/.; done < /home/plenzini/tools/catTLBX/datalist.csv').stdout.read()
    run_ssh_cmd(username,
                'for f in /home/plenzini/tools/catTLBX/cache/*\ *; do mv "$f" "${f// /_}"; done').stdout.read()
    run_ssh_cmd(username,
                'find /home/plenzini/tools/catTLBX/cache/ -type f > /home/plenzini/tools/catTLBX/datalist2.csv').stdout.read()
    run_ssh_cmd(username,
                "sed -i 's/\/home\/plenzini/\/Users\/plenzini\/chpc3/g' /home/plenzini/tools/catTLBX/datalist2.csv").stdout.read()
    # Using readlines()
    file1 = open('/Users/plenzini/chpc3/tools/catTLBX/datalist2.csv', 'r')
    Lines = file1.readlines()
    sitedf=pd.DataFrame()
    count = 0
    # Strips the newline character
    for line in Lines:
        #try:
            #count += 1
            #subsetdf=pd.read_csv(line.strip("\n"))
            #sitedf=pd.concat([sitedf,subsetdf],axis=0)
            #sitedf['sitestr']=siteabbrev
        #except Exception as e:
            #print("Error processing file:", line.strip())
            #print("Exception:", str(e))
        count += 1
        subsetdf=pd.read_csv(line.strip("\n"))
        sitedf=pd.concat([sitedf,subsetdf],axis=0)
        sitedf['sitestr']=siteabbrev
    file1.close()
    run_ssh_cmd('plenzini@login3.chpc.wustl.edu',
                "rm -f /home/plenzini/tools/catTLBX/cache/* /home/plenzini/tools/catTLBX/datalist* ").stdout.read()


#    run_ssh_cmd(username,
#                "rm -f /home/w.zijian/tools/catTLBX/cache/* /home/w.zijian/tools/catTLBX/datalist* ").stdout.read()

    return sitedf


def getPCP(intradb,project,pipeline):
    # TO TO: curl this with a JSESSION ID instead:
    url = 'https://intradb.humanconnectome.org/xapi/pipelineControlPanel/project/'+project+'/pipeline/'+pipeline+'/status'
    params = {
        'condensed': 'true',
        'cached': 'true',
        'dontWait': 'true',
        'emailForEarlyReturn': 'false',
        'limitedRefresh': 'false'
    }
    headers = {
        'Accept': 'application/json'
    }
    user=intradb.user[0]
    passw=intradb.auth[0]
    response = requests.get(url, params=params, headers=headers, auth=(user,passw))

    if response.status_code == 200:
        data = response.json()
        # Process the JSON data here
    else:
        print("Request failed with status code:", response.status_code)
    ###############
    PCP = pd.DataFrame.from_dict(data)#[['entityLabel', 'validated', 'issues','project']]
    PCP['PIN'] = PCP.entityLabel.str.replace('_MR', '')
    PCP['subject'] = PCP.PIN.str.split('_', expand=True)[0]
    PCP['redcap_event'] = PCP.PIN.str.split('_', expand=True)[1]

    return PCP #pd.DataFrame.from_dict(data).columns


def filterdupass(instrument,dupvar,iset,dset):
    fixass=iset[['subject','subject_id', 'study_id', 'redcap_event','redcap_event_name', 'site','v0_date','event_date',dupvar]].copy()
    fixass['reason']='Duplicated Assessments'
    fixass['code']='orange'
    fixass['PIN']=fixass.subject + '_' + fixass.redcap_event
    fixass=fixass.loc[~(fixass[dupvar]=='')][['PIN',dupvar]]
    fixass[dupvar]=fixass[dupvar].str.upper().str.replace('ASSESSMENT','').str.strip() #dont want to remove all alphanumeric...need more control over conventions
    check1 = fixass.loc[fixass[dupvar] == '1 AND 2'].copy()
    onetwo=''
    try:
        onetwo=list(check1.PIN)[0]
    except:
        pass
    #fixass=pd.concat([check,fixass.loc[~(fixass[dupvar] == '1 AND 2')]])
    fixass['Assessment Name']="Assessment " + fixass[dupvar]
    fixass['Inst']=instrument
    dset=pd.merge(dset,fixass,on=['PIN','Inst','Assessment Name'],how='left')
    dset=dset.loc[~(dset[dupvar].isnull()==False)]
    dset=dset.loc[~((dset['Assessment Name']=="Assessment 1") & (dset.Inst==instrument) & (dset.PIN==onetwo))]
    dset=dset.loc[~((dset['Assessment Name'] == "Assessment 2") & (dset.Inst == instrument) & (dset.PIN == onetwo))]
    return dset

def getredcap10Q(struct,studystr,curatedsnaps,goodies,idstring,config,restrictedcols=[]): #secret,config,
    """
    downloads all events and fields in a redcap database
    """
    #studydata = pd.DataFrame()
    #auth = secret
    #print(auth)
    #token=auth.loc[auth.source==studystr,'api_key'].reset_index().api_key[0]
    ##subj=auth.loc[auth.source==studystr,'field'].reset_index().field[0]
    subj=config['Redcap']['datasources']['qint']['subject']
    #print(token)
    #print(subj)
    idvar='id'
    data=struct
    #data = {
    #    'token': token,
    #    'content': 'record',
    #    'format': 'json',
    #    'type': 'flat',
    #    'rawOrLabel': 'raw',
    #    'rawOrLabelHeaders': 'raw',
    #    'exportCheckboxLabel': 'false',
    #    'exportSurveyFields': 'false',
    #    'exportDataAccessGroups': 'false',
    #    'returnFormat': 'json'
    #}
    #buf = BytesIO()
    #ch = pycurl.Curl()
    #ch.setopt(ch.URL, 'https://redcap.wustl.edu/redcap/api/')
    #ch.setopt(ch.HTTPPOST, list(data.items()))
    #ch.setopt(ch.WRITEDATA, buf)
    #ch.perform()
    #ch.close()
    #htmlString = buf.getvalue().decode('UTF-8')
    #buf.close()
    #df = pd.read_json(htmlString)
    df=getframe(data,config['Redcap']['api_url'])
    print(df.shape)
    if (studystr=='qint'):
        print('Dropping unusuable Q records')
        print(df.shape)
        df=df.loc[~(df.q_unusable=='1')]
        print(df.shape)
        df['subject']=df[subj]
        df['redcap_event']='V'+df.visit.astype('str')
        #df.loc[df.redcap_event=='VCR','redcap_event']='CR'
        #if(idstring=='HCD'):
        #    df=df.loc[df[subj].str.contains('HCD')].copy()
        #    df = df.loc[~(df.assessment.str.contains('RAVLT'))].copy()
        #    cols = [c for c in df.columns if c.lower()[:5] != 'ravlt']
        #    df = df[cols].copy()
        if(idstring=='HCA'):
            df=df.loc[df[subj].str.contains('HCA')]
            df = df.loc[df.assessment.str.contains('RAVLT')].copy()
            print(len(df.columns))
            cols = [c for c in df.columns if c.lower()[:4] != 'wais']
            cols = [c for c in cols if c[:4] != 'wisc']
            cols = [c for c in cols if c[:4] != 'wpps']
            print(len(cols))
            df = df[cols].copy()
    print('Dropping subs with issues')
    #for sb in list(flaggedgold.subject):
    df=df.loc[(df[subj].str[:10].isin(goodies))].copy()
    df=df.loc[~(df[subj].str.contains('CC'))].copy()

    print(df.shape)
    if (studystr=='qint'):
        dfrestricted=df.copy() #[['id', 'subjectid', 'visit']+restrictedcols]
    for dropcol in restrictedcols:
        #try:
        df=df.drop(columns=dropcol)
        #except:
        #    pass
    print(df.shape)
    return df, dfrestricted

def getfullRedExport(studystr,auth):
    """
    downloads all events and fields in a redcap database
    """
    studydata = pd.DataFrame()
    print(auth)
    token=auth.loc[auth.source==studystr,'api_key'].reset_index().api_key[0]
    print(token)
    data = {
        'token': token,
        'content': 'record',
        'format': 'json',
        'type': 'flat',
        'rawOrLabel': 'raw',
        'rawOrLabelHeaders': 'raw',
        'exportCheckboxLabel': 'false',
        'exportSurveyFields': 'false',
        'exportDataAccessGroups': 'false',
        'returnFormat': 'json'
    }
    df = getframe(data, config['Redcap']['api_url'])
    return df

def folder_files(client, folders, extension='.csv', recursively=False):
    """
    A legacy function.
    """
    result = {}

    for folder_id in folders:
        #client = box.get_client()
        f = client.folder(folder_id)
        print('Scanning %s' % folder_id)
        print(dir(f))
        print('.', end='')
        items = list(f.get_items())

        folders = []
        #files = {}

        for i in items:
            #if i.type == 'file':
            #    if i.name.endswith(extension):
            #        files[i.id] = {
            #            'filename': i.name,
            #            'fileid': i.id,
            #            'sha1': i.sha1
            #        }
            if i.type == 'folder':
                folders.append(i.id)

        #result.update(files)

        #if recursively:
            #res2=box.list_of_files(folders)
            #folderdeet=box.folder_info(int(folders[0])
            #result.update(box.list_of_files(folders, extension, True))

    return folders #, result

def removeIssues(dataset,issuefila,component='ASA24'):
    issues=pd.read_csv(issuefila)#'All_Issues_'+date.today().strftime("%d%b%Y")+'.csv'
    droplist=issues.loc[issues.datatype==component][['subject','redcap_event']].drop_duplicates()
    dset=pd.merge(dataset,droplist,on=['subject','redcap_event'],how='outer',indicator=True)
    return dset.loc[dset._merge=='left_only'].drop(columns='_merge')

def droprest(datain,dropvars):
    return datain.drop(columns=dropvars)

###USE PRESTONS VERSION
#def PINfirst(dataset,strname,issuefile,inventory,dropvars):
#    dataset['subject'] = dataset.PIN.str[:10]
#    dataset['redcap_event'] = dataset.PIN.str[11:13]
#    print(dataset.shape)
#    dataset2=pd.merge(dataset,inventory,on=['subject','redcap_event'],how='inner')
#    print(issuefile)
#    dataset3=removeIssues(dataset2,component='ASA24',issuefila=issuefile)
#    a = list(dataset3.columns)
#    b = dataset3[a[-3:] + a[:-3]].sort_values('PIN').drop_duplicates()
#    print(b.shape)
#    c = b.drop(columns=dropvars)
#    c.loc[b.PIN != ''].to_csv("./tmp/AABC_ASA24-" + strname + "_" + date.today().strftime("%Y-%m-%d") + ".csv",index=False)
#    return b,c

def PINfirst(dataset,strname,issuefile,inventory,dropvars):
    dataset['subject'] = dataset.PIN.str[:10]
    dataset['redcap_event'] = dataset.PIN.str[11:13]
    print(dataset.shape)
    dataset2=pd.merge(dataset,inventory,on=['subject','redcap_event'],how='inner')
    dataset3=removeIssues(dataset2,component='ASA24',issuefile=issuefile)
    a = list(dataset3.columns)
    b = dataset3[a[-3:] + a[:-3]].sort_values('PIN').drop_duplicates()
    print(b.shape)
    #for the restricted folder
    b.loc[b.PIN != ''].to_csv("./tmp/AABC_ASA24-"+ strname +"_Restricted_" + date.today().strftime("%Y-%m-%d") + '.csv', index=False)
    #for the regular folder
    c = b.drop(columns=dropvars)
    c.loc[b.PIN != ''].to_csv("./tmp/AABC_ASA24-" + strname + "_" + date.today().strftime("%Y-%m-%d") + ".csv",index=False)
    return b,c




#subsetfreeze
def subsetfreeze(df,strname,listfreeze,byPIN=True,bySUB=False):
    if byPIN:
        dff=df.loc[df.PIN.isin(listfreeze)]
    if bySUB:
        dff=df.loc[df.subject.isin(listfreeze)]
    dff.loc[dff.PIN != ''].to_csv("./tmp/Freeze1_AABC_ASA24-" + strname + "_" + date.today().strftime("%Y-%m-%d") + ".csv",index=False)
    return dff



def getASA(client,folderqueue):
    Totals = pd.DataFrame();
    #TNS = pd.DataFrame(); INS = pd.DataFrame(); TS = pd.DataFrame(); Totals = pd.DataFrame(); Items = pd.DataFrame(); Resp = pd.DataFrame()
    allsubdb = pd.DataFrame(); CORRUPT=pd.DataFrame();ALLSUBS=pd.DataFrame();
    BIGGESTTotals = pd.DataFrame();
    #BIGGESTTNS = pd.DataFrame(); BIGGESTINS = pd.DataFrame(); BIGGESTTS = pd.DataFrame(); BIGGESTTotals = pd.DataFrame(); BIGGESTItems = pd.DataFrame(); BIGGESTResp = pd.DataFrame()

    for studyshort in folderqueue:
        print("Study:",studyshort)
        folder = config['NonQBox']['ASA24'][studyshort]
        dag = config['Redcap']['datasources']['aabcarms'][studyshort]['dag']
        sitenum = config['Redcap']['datasources']['aabcarms'][studyshort]['sitenum']
        subfolders = folder_files(client, [folder])
        for i in subfolders:#[0:3]:
            if studyshort=='UCLA':
                subsubfolders = folder_files(client, [i])
                for j in subsubfolders:  # [0:3]:
                    subfilelist = box.list_of_files([j])
                    f = client.folder(folder_id=j).get()
                    subdb = pd.DataFrame(subfilelist).transpose()
                    subdb['PIN'] = str(f)[str(f).find('HC'):str(f).find('HC') + 13].strip(' ')
                    print(studyshort+":"+j)
                    allsubdb = allsubdb.append(subdb)
            else:
                subfilelist = box.list_of_files([i])
                f = client.folder(folder_id=i).get()
                subdb = pd.DataFrame(subfilelist).transpose()
                subdb['PIN'] = str(f)[str(f).find('HC'):str(f).find('HC') + 13].strip(' ')
                allsubdb = allsubdb.append(subdb)
        ALLSUBS = ALLSUBS.append(allsubdb)
        print("shape", ALLSUBS.shape)
        #dbitemsTNS = allsubdb.loc[allsubdb.filename.str.contains('TNS')].copy()
        #dbitemsINS = allsubdb.loc[allsubdb.filename.str.contains('INS')].copy()
        #dbitemsTS = allsubdb.loc[allsubdb.filename.str.contains('TS')].copy()
        dbitemsTotals = allsubdb.loc[allsubdb.filename.str.contains('Totals')].copy()
        #dbitemsItems = allsubdb.loc[allsubdb.filename.str.contains('Items')].copy()
        #dbitemsResp = allsubdb.loc[allsubdb.filename.str.contains('Responses')].copy()
        #TNS = TNS.append(dbitemsTNS)
        #INS = INS.append(dbitemsINS)
        #TS = TS.append(dbitemsTS)
        Totals = Totals.append(dbitemsTotals)
        #Items = Items.append(dbitemsItems)
        #Resp = Resp.append(dbitemsResp)

        Corrupted1 = pd.DataFrame();Corrupted2 = pd.DataFrame();Corrupted3 = pd.DataFrame();Corrupted4 = pd.DataFrame();Corrupted5 = pd.DataFrame();Corrupted6 = pd.DataFrame();
        BigTotals = pd.DataFrame()
        #BigItems = pd.DataFrame()
        #BigResp = pd.DataFrame()
        #BigTNS = pd.DataFrame()
        #BigINS = pd.DataFrame()
        #BigTS = pd.DataFrame()

        for f in list(Totals.fileid):
            try:
                k = box.read_csv(f)
                if not k.empty:
                    k['PIN'] = Totals.loc[Totals.fileid == f, "PIN"][0]
                    BigTotals = BigTotals.append(k)
            except:
                print("Couldn't read", f)
                #corrupt['f'] = f
                #corrupt['PIN'] = Totals.loc[Totals.fileid == f, "PIN"][0]
                #Corrupted1 = Corrupted1.append(corrupt)
        #for f in Items.fileid:
        #    try:
        #        k = box.read_csv(f)
        #        if not k.empty:
        #            k['PIN']= Items.loc[Items.fileid == f, "PIN"][0]
        #            BigItems = BigItems.append(k)
        #    except:
        #        print("Couldn't read", f)
        #        #corrupt['f'] = f
        #        #corrupt['PIN'] = Items.loc[Items.fileid == f, "PIN"][0]
        #        #Corrupted2 = Corrupted2.append(corrupt)
        #for f in Resp.fileid:
        #    try:
        #        k = box.read_csv(f)
        #        if not k.empty:
        #            k['PIN'] = Resp.loc[Resp.fileid == f, "PIN"][0]
        #            BigResp = BigResp.append(k)
        #    except:
        #        print("Couldn't read", f)
        #        #corrupt['f'] = f
        #        #corrupt['PIN'] = Resp.loc[Resp.fileid == f, "PIN"][0]
        #        #Corrupted3 = Corrupted3.append(corrupt)
        #for f in TNS.fileid:
        #    try:
        #        k = box.read_csv(f)
        #        if not k.empty:
        #            k['PIN'] = TNS.loc[TNS.fileid == f, "PIN"][0]
        #            BigTNS = BigTNS.append(k)
        #    except:
        #        print("Couldn't read", f)
        #        #corrupt['f'] = f
        #        #corrupt['PIN'] = TNS.loc[TNS.fileid == f, "PIN"][0]
        #        #Corrupted4 = Corrupted4.append(corrupt)
        #for f in INS.fileid:
        #    try:
        #        k = box.read_csv(f)
        #        if not k.empty:
        #            k['PIN'] = INS.loc[INS.fileid == f, "PIN"][0]
        #            BigINS = BigINS.append(k)
        #    except:
        #        print("Couldn't read", f)
        #        #corrupt['f'] = f
        #        #corrupt['PIN'] = INS.loc[INS.fileid == f, "PIN"][0]
        #        #Corrupted5 = Corrupted5.append(corrupt)
        #for f in TS.fileid:
        #    try:
        #        k = box.read_csv(f)
        #        if not k.empty:
        #            k['PIN'] = TS.loc[TS.fileid == f, "PIN"][0]
        #            BigTS = BigTS.append(k)
        #    except:
        #        print("Couldn't read", f)
        #        #corrupt['f'] = f
        #        #corrupt['PIN'] = TS.loc[TS.fileid == f, "PIN"][0]
        #        #Corrupted6 = Corrupted6.append(corrupt)
        ##CORRUPT = pd.concat([CORRUPT,Corrupted1,Corrupted2,Corrupted3,Corrupted4,Corrupted5,Corrupted6],axis=0)
        ##print("Study:", studyshort)
        if not BigTotals.empty:
            BIGGESTTotals=BIGGESTTotals.append(BigTotals)
        #if not BigItems.empty:
        #    BIGGESTItems = BIGGESTItems.append(BigItems)
        #if not BigTNS.empty:
        #    BIGGESTTNS=BIGGESTTNS.append(BigTNS)
        #if not BigINS.empty:
        #    BIGGESTINS = BIGGESTINS.append(BigINS)
        #if not BigTS.empty:
        #    BIGGESTTS = BIGGESTTS.append(BigTS)
        #if not BigResp.empty:
        #    BIGGESTResp = BIGGESTResp.append(BigResp)

    return ALLSUBS, BIGGESTTotals
    #return ALLSUBS, BIGGESTTotals,BIGGESTItems,BIGGESTResp,BIGGESTTS,BIGGESTTNS,BIGGESTINS

#PRESTONs ASA_3
def getASA_3(client, folderqueue):
    start_time = time.time()
    allsubdb_list = []
    big_data = {'Totals': [], 'Items': [], 'Resp': [], 'TNS': [], 'INS': [], 'TS': []}

    total_files_processed = 0  # For percentage calculation
    total_files = 3119
    pbar = tqdm(total=total_files, desc="Processing files")
   
    for studyshort in folderqueue:
        print("Study:", studyshort)
        folder = config['NonQBox']['ASA24'][studyshort]
        dag = config['Redcap']['datasources']['aabcarms'][studyshort]['dag']
        sitenum = config['Redcap']['datasources']['aabcarms'][studyshort]['sitenum']
    
        if studyshort == 'UCLA':
            # Handle UCLA's additional layer of folders
            subfolders = folder_files(client, [folder])
            for subfolder in subfolders:
                subsubfolders = folder_files(client, [subfolder])
                for subsubfolder in subsubfolders:
                    process_folder(subsubfolder, big_data, studyshort)
                    #total_files_processed += 1
                    pbar.update(6)

        else:
            # Directly process folders for other study sites
            subfolders = folder_files(client, [folder])
            for subfolder in subfolders:
                print(subfolder)
                process_folder(subfolder, big_data, studyshort)
                #total_files_processed += 1
                pbar.update(6)
        print(f"Completed processing for {studyshort}.")
    pbar.close()

    # Calculate and print running time
    end_time = time.time()
    running_time = end_time - start_time
    print(f"Total running time: {running_time:.2f} seconds")



    # Concatenate all DataFrames collected for each category
    result_dfs = {category: pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
                  for category, dfs in big_data.items()}

    return result_dfs


def process_folder(folder_id, big_data, studyshort):
    subfilelist = box.list_of_files([folder_id])
    #subfolders_id = folder_files(client, [folder_id])

    for filename in subfilelist:

        # Skip .zip files for UCLA
        if studyshort == 'UCLA' and filename.endswith('.zip'):
            print(f"Skipping .zip file: {filename}")

            continue

        # Download and process file based on its category
        f = box.downloadFile(filename, download_dir="tmp", override_if_exists=True)
        category = determine_category(f)
        if category:
            try:
                # Assuming a function to process the file and return a DataFrame
                df = process_file(f, category, filename)
                pin = client.folder(folder_id=folder_id).get()
                print(pin)
                df['PIN'] = str(pin)[str(pin).find('HC'):str(pin).find('HC') + 13].strip(' ')
                big_data[category].append(df)

            except Exception as e:
                print(f"Error processing file: {filename}, Error: {e}")

def determine_category(filename):
    # Determine file's category based on its name
    if 'Totals' in filename:
        return 'Totals'
    elif 'Items' in filename:
        return 'Items'
    elif 'Resp' in filename:
        return 'Resp'
    elif 'TNS' in filename:
        return 'TNS'
    elif 'INS' in filename:
        return 'INS'
    elif 'TS' in filename:
        return 'TS'
    else:
        return None

def process_file(filepath, category, filename):
    # Process the downloaded file and return a DataFrame
    # This is a placeholder; actual processing will depend on file content and format
    print(f"Processing {category} file: {filename}")

    df = pd.read_csv(filepath)
   
    return df

def mapping_list(df, prefixes):
    """
    Showing the mapping list

    Parameters:
    df (pd.DataFrame): The input DataFrame.
    prefixes (list): A list of prefixes to identify the columns to be merged.

    Returns:
    mapping: The list with the merged columns.
    """
    # Extract the relevant column containing the variable names
    variable_names = df['Variable / Field Name']

    # Create a mapping from prefixed variables to their base variables
    mapping = {}
    for var in variable_names:
        if any(var.startswith(prefix) for prefix in prefixes):
            base_var = var.split('_', 1)[1]
            mapping[var] = base_var
    return mapping

def merge_prefixed_columns(df, prefixes):
    """
    Merges columns in the DataFrame that start with any of the specified prefixes with their corresponding columns.

    Parameters:
    df (pd.DataFrame): The input DataFrame.
    prefixes (list): A list of prefixes to identify the columns to be merged.

    Returns:
    pd.DataFrame: The DataFrame with the merged columns.
    """
    # Create a copy of the DataFrame to avoid modifying the original
    df_merged = df.copy()

    # Iterate over each prefix
    for prefix in prefixes:
        # Identify columns starting with the prefix
        prefixed_columns = [col for col in df.columns if col.startswith(prefix)]

        # Iterate over each prefixed column
        for prefixed_col in prefixed_columns:
            # Determine the corresponding column name by removing the prefix
            base_col = prefixed_col[len(prefix):]

            # Check if the corresponding column exists in the DataFrame
            if base_col in df.columns:
                # Merge the columns by taking the non-null values from either column
                df_merged[base_col] = df_merged[prefixed_col].combine_first(df_merged[base_col])
                # Drop the prefixed column after merging
                df_merged.drop(columns=[prefixed_col], inplace=True)

    return df_merged


def clean_croms_income(value):
    import numpy as np
    # Remove punctuation (commas, periods)
    value = re.sub(r'[,.]', '', str(value)).strip()

    # Handle empty values and -9999 as missing data
    if value == '' or value == '-9999' or value == '-999':
        return np.nan

    # If it's a range, calculate the average
    if '-' in value:
        try:
            low, high = map(int, value.split('-'))
            return (low + high) / 2
        except ValueError:
            return np.nan  # In case the range contains invalid values
    else:
        try:
            return int(value)
        except ValueError:
            return np.nan  # In case it's not a valid number


#def swapmeds(oldDF=HCPDparentmeds, medmap=MAP, medvarlist=medlist):
def swapmeds(oldDF, medmap, medvarlist):
    ### Function maps a list of medication typos (or some other map dictionary) to a clean df of medication names or a yes/no class (or whatever is being replaced).
    DP=oldDF.copy()
    for m in medvarlist:
        print(m,'before clean:',str(len(DP[m].unique())))
        DP[m]=DP[m].str.upper().str.strip()
        DP[m] = DP[m].str.upper().str.strip().map(medmap)
        print(m,'after clean:',str(len(DP[m].unique())))
    return DP
