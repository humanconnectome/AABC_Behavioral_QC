import pandas as pd
import yaml
import ccf
from ccf.box import LifespanBox
import requests
import re
import collections
import subprocess
import os
import sys
from subprocess import Popen, PIPE

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
                                           config['Redcap']['datasources']['aabcarms']['eventmap']})['redcap_event_name']
    idvisit = idvisit.loc[~(idvisit.subject.astype(str).str.upper().str.contains('TEST'))]
    return idvisit

def parse_content(content):
    section_headers = [
        'Subtest,,Raw score',
        'Subtest,,Scaled score',
        'Subtest,Type,Total',  # this not in aging or RAVLT
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
    cmds = ['ssh', '-t', host, cmd]
    return Popen(cmds, stdout=PIPE, stderr=PIPE, stdin=PIPE)


def getlist(mask,sheet):
    restrictA=pd.read_excel(mask, sheet_name=sheet)
    restrictedA=list(restrictA.field_name)
    return restrictedA

def TLBXreshape(results1):
    df=results1.decode('utf-8')
    df=pd.DataFrame(str.splitlines(results1.decode('utf-8')))
    df=df[0].str.split(',', expand=True)
    cols=df.loc[df[0]=='PIN'].values.tolist()
    df2=df.loc[~(df[0]=='PIN')]
    df2.columns=cols[0]
    return df2


def getredcap10Q(studystr,curatedsnaps,goodies,idstring,restrictedcols=[]):
    """
    downloads all events and fields in a redcap database
    """
    df=getframe(struct, api_url)
    print(df.shape)
    if (studystr=='qint'):
        print('Dropping unusuable Q records')
        print(df.shape)
        df=df.loc[~(df.q_unusable=='1')]
        print(df.shape)
        df['subject']=df[subj]
        df['redcap_event']='V'+df.visit.astype('str')
    print(df.shape)
    print('Dropping exclusions/DNRs/Withdrawns')
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
