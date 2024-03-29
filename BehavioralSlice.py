from ccf.box import LifespanBox
import pandas as pd
import matplotlib.pyplot as plt
import os
import shutil
from datetime import date
from config import *
import numpy as np

# TO DO: turn special lists into config stuff
v2oops=['HCA6686191','HCA7296183'] # Minimal HCA behavioral-only visit data for subjects that came in as v2 in AABC
v2oopsexp=['HCA6686191_V2', 'HCA7296183_V2','HCA6686191_F2', 'HCA7296183_F2','HCA6686191_F3', 'HCA7296183_F3','HCA6686191_Covid', 'HCA7296183_Covid','HCA6686191_CR', 'HCA7296183_CR']
freeze1Subs=list(pd.read_csv('/Users/petralenzini/work/Behavioral/AABC/AABC_Behavioral_QC/AABC_Behavioral_QC/tmp/Freeze1_n439_12Feb2024.csv',header=None)[0])
PCMPlist=['HCA6426670','HCA6732980','HCA6780789','HCA6878504','HCA7025859','HCA7126865','HCA7411864','HCA8043765','HCA8343272','HCA8540678','HCA8715687','HCA8738598','HCA8858104','HCA8935904','HCA9137577','HCA9808700','HCA9814189']

# TO DO: map events to harmonized version (e.g. for FU and Covid HCA stuff)
##########################################################################
# Output is a table, slice dictionary, plots, distributions, and receipt

# Recruitment pdf (always the latest because synced to BOX from local machine)
# replace with config location
statsdir='/Users/petralenzini/work/Behavioral/AABC/AABC_Behavioral_QC/AABC_Behavioral_QC/tmp/'
statsfile='AABC_HCA_recruitmentstats.pdf'
gwasfamfile=statsdir+"HCA_imputed_geno0.02_final.fam"
gwas=list(pd.read_csv(os.path.join(statsdir,gwasfamfile),sep='\t',header=None)[0].unique()) #949

# Now complete the following
datarequestor='Nichols_CogFactorGen'  # name of folder in which you want to generate the slice.
study="HCA-AABC" #or HCA
#modify this list (many examples commented out):
InstRequested=['Completeness Inventory','NIH Toolbox Pattern Comparison Processing Speed Test','NIH Toolbox Picture Sequence Memory Test','NIH Toolbox Picture Vocabulary Test','NIH Toolbox Visual Acuity Test','NIH Toolbox Words-In-Noise Test','NIH Toolbox Dimensional Change Card Sort Test','NIH Toolbox Flanker Inhibitory Control and Attention Test','NIH Toolbox List Sorting Working Memory Test','NIH Toolbox Oral Reading Recognition Test','Cognition Crystallized Composite','Cognition Fluid Composite','Cognition Total Composite Score','Montreal Cognitive Assessment (MOCA)','Trail Making Scores','Q-Interactive Ravlt']
BulkRequested=[]#'Pre-Processed Imaging Sessions','Vasomotor Symptoms (VMS - Raw Bulk)']
DerivativesRequested=[]#['Cognition Factor Analysis', 'Cardiometabolic Index and Allostatic Load','Imaging Derived Phenotypes']
OtherInstruments=[]#'Hormones (FSH, LH, testosterone, estradiol)']
IndividVars=[]


#Do you want caveman distributions of plots?
wantplots = False

#Are you an administrator with API credentials in the Pre-Release BOX folder?
isAdmin = True
DownloadF = False
#########################################################################################################
################################### Done with user specs #####################

InstRequest1=['Completeness Inventory']+InstRequested
InstRequest=list(pd.DataFrame(InstRequest1).drop_duplicates()[0])

if isAdmin:
    try:
        os.mkdir(os.path.join(os.getcwd(),datarequestor))
        os.mkdir(os.path.join(os.getcwd(),datarequestor+'/downloadedfiles/'))
    except:
        print("couldn't make folder for datarequestor: [",datarequestor,"]")
    try:
        config = LoadSettings()
        secret=pd.read_csv(config['config_files']['secrets'])
        #get list of files in Box folders (interative)
        box = LifespanBox(cache=os.path.join(os.getcwd(),datarequestor+'/downloadedfiles/'))
        hca_pre=box.list_of_files([str(config['hca_pre'])])
        hca_restricted=box.list_of_files([str(config['hca_pre_restricted'])])
        aabc_pre=box.list_of_files([str(config['aabc_pre'])])
        E=pd.read_csv(box.downloadFile(config['encyclopedia']),low_memory=False,encoding='ISO-8859-1')
        Evars = E.loc[(E['Form / Instrument'].isin(InstRequest)) | (E['Variable / Field Name'].isin(IndividVars))]
        Evars = Evars.loc[~(Evars.Unavailable == 'U')]
        t2 = Evars.loc[Evars.Unavailable == 'U']
        droplist=list(t2['Variable / Field Name'].unique())

        print("Number of Requested Instruments:",len(InstRequested))
        print("Number of Instruments Found:",len(Evars['Form / Instrument'].unique()))

        #whittle down the list of files in the Pre-Release folder to the most recent subset
        hcafiles=pd.DataFrame.from_dict(hca_pre, orient='index')
        hcafiles=hcafiles.loc[~(hcafiles.filename.str.upper().str.contains('DICTIONARY') | hcafiles.filename.str.upper().str.contains('ENCYCLOPEDIA'))].copy()
        hcafiles['datestamp']=hcafiles.filename.str.split('_',expand=True)[2]
        hcafiles['datatype']=hcafiles.filename.str.split('_',expand=True)[1]
        hcafiles=hcafiles.loc[(hcafiles.datestamp.str.contains('.csv')==True) & (hcafiles.datestamp.str.contains('-'))].copy()
        hcafiles.datestamp=hcafiles.datestamp.str.replace('.csv','')
        hcafiles.datestamp=pd.to_datetime(hcafiles.datestamp)
        hcafiles=hcafiles.sort_values('datestamp',ascending=False)
        hcafiles=hcafiles.drop_duplicates(subset='datatype',keep='first').copy()

        hcaRfiles=pd.DataFrame.from_dict(hca_restricted, orient='index')
        hcaRfiles=hcaRfiles.loc[~(hcaRfiles.filename.str.upper().str.contains('DICTIONARY') | hcaRfiles.filename.str.upper().str.contains('ENCYCLOPEDIA') | hcaRfiles.filename.str.upper().str.contains('FILTERED') | hcaRfiles.filename.str.upper().str.contains('RESTRICTED_REDCAP'))].copy()
        hcaRfiles['datestamp']=hcaRfiles.filename.str.split('_',expand=True)[3]
        hcaRfiles['datatype']=hcaRfiles.filename.str.split('_',expand=True)[1]
        hcaRfiles=hcaRfiles.loc[(hcaRfiles.datestamp.str.contains('.csv')==True) & (hcaRfiles.datestamp.str.contains('-'))].copy()
        hcaRfiles.datestamp=hcaRfiles.datestamp.str.replace('.csv','')
        hcaRfiles.datestamp=pd.to_datetime(hcaRfiles.datestamp)
        hcaRfiles=hcaRfiles.sort_values('datestamp',ascending=False)
        hcaRfiles=hcaRfiles.drop_duplicates(subset='datatype',keep='first').copy()
        hcaRfiles['dropRlist'] = hcaRfiles.filename.str.replace('_Restricted', '')
        #also need to drop old APOE file in favor of new list
        dropRfiles=list(hcaRfiles.dropRlist.unique())+['HCA_Apoe-Isoforms_2022-03-03.csv']

        aabcfiles=pd.DataFrame.from_dict(aabc_pre, orient='index')
        aabcfiles['datestamp']=aabcfiles.filename.str.split('_',expand=True)[2]
        aabcfiles['datatype']=aabcfiles.filename.str.split('_',expand=True)[1]
        aabcfiles=aabcfiles.loc[(aabcfiles.datestamp.str.contains('.csv')==True) & (aabcfiles.datestamp.str.contains('-'))].copy()
        aabcfiles.datestamp=aabcfiles.datestamp.str.replace('.csv','')
        aabcfiles.datestamp=pd.to_datetime(aabcfiles.datestamp)
        aabcfiles=aabcfiles.sort_values('datestamp',ascending=False)
        aabcfiles=aabcfiles.drop_duplicates(subset='datatype',keep='first').copy()

        #download
        for i in list(aabcfiles.fileid)+list(hcafiles.fileid) +list(hcaRfiles.fileid):
            if DownloadF:
                print("downloading",i,"...")
                box.downloadFile(i)
            else:
                print("already downloaded",i,"...")
    except:
        print("Something went wrong")
##### end Admin section
# Loop through downloaded files and grab any of the Instruments therein, by study.
# THen stack studies together, incorporating exceptions for APOE, FAMILIES, and REgistration variables
widefileAABC=pd.DataFrame(columns=['study','subject','redcap_event'])
widefileHCA=pd.DataFrame(columns=['study','subject','redcap_event'])
#i='HCA_NIH-Toolbox-Scores_2022-01-28.csv'
i='AABC_NIH-Toolbox-Scores_2024-03-21.csv'
for i in os.listdir(os.path.join(os.getcwd(),datarequestor+'/downloadedfiles/')):
    if i in dropRfiles:
        print("skipping",i,"in favor of AABC combined or HCA restricted version")
    else:
        Evars2=list(Evars['Variable / Field Name'])+IndividVars
        if 'NIH' in i and 'Scores' in i:
            print(i)
            bulk=pd.read_csv(os.path.join(os.getcwd(),datarequestor+'/downloadedfiles/'+i),low_memory=False)
            l=[]
            tlbxlist = [j for j in InstRequest if
                        ('NIH' in j) or ('Cognition' in j and 'Composite' in j) or ('Summary (18+)' in j)]
            for t in tlbxlist:
                l2=[b for b in bulk.Inst.unique() if t in b]
                l=l+l2
            bulk=bulk.loc[bulk.Inst.isin(l)].copy()
            if not bulk.empty:
                bulk['subject']=bulk.PIN.str.split('_',expand=True)[0]
                bulk['redcap_event']=bulk.PIN.str.split('_',expand=True)[1]
                for k in tlbxlist:
                    print(k)
                    tlbxvars=Evars.loc[Evars['Form / Instrument']==k][['Variable / Field Name','NIH Toolbox Prefix in Slice','Unavailable']]
                    tlbxvars['newname']=tlbxvars['NIH Toolbox Prefix in Slice']+'_'+tlbxvars['Variable / Field Name']
                    tlbxvars['newname']=tlbxvars.newname.str.replace(' ','_')
                    mapvars=dict(zip(list(tlbxvars['Variable / Field Name']),list(tlbxvars.newname)))#,tlbxvars['newname'])
                    temp=bulk.loc[bulk.Inst.str.contains(k)][['subject','redcap_event']+list(tlbxvars['Variable / Field Name'])]
                    temp=temp.rename(columns=mapvars)
                    for j in ['site', 'study', 'PIN', 'redcap_event_name', 'site', 'study_id', 'id', 'gender']:
                        try:
                            temp = temp.drop(columns=[j]).copy()
                        except:
                            pass
                    if 'AABC' in i:
                        widefileAABC=pd.merge(widefileAABC,temp, on=['subject','redcap_event'],how='outer')
                    if 'HCA' in i:
                        widefileHCA = pd.merge(widefileHCA, temp, on=['subject', 'redcap_event'], how='outer')
        if 'Inventory' in i:
            print(i)
            temp = pd.read_csv(os.path.join(os.getcwd(),datarequestor+'/downloadedfiles/' + i), low_memory=False)
            subtempA = temp[temp.columns.intersection(set(['subject', 'redcap_event'] + Evars2))]
            dropvars=['pedid','nda_age','nda_interview_date','pseudo_guid','DB_Source','HCAid','HCDid','legacy_yn','sub_event','daysfromtime0','REDCap_id','study_id','psuedo_guid']
            keepvars=[i for i in list(subtempA.columns) if i not in dropvars]
            subtempB = subtempA[keepvars].copy()
            if 'AABC' in i:
                widefileAABC = pd.merge(widefileAABC, subtempB, on=['subject', 'redcap_event'], how='outer')
            if 'HCA' in i:
                widefileHCA = pd.merge(widefileHCA, subtempB, on=['subject', 'redcap_event'], how='outer')

        if 'Metabolites' not in i and 'Inventory' not in i and 'Apoe' not in i and 'Encyclopedia' not in i and 'NIH' not in i and 'Composite' not in i and ".DS_Store" not in i and '-INS' not in i and '-Resp' not in i and '-Items' not in i and '-TS' not in i and '-TNS' not in i:
            print(i)
            try:
                temp=pd.read_csv(os.path.join(os.getcwd(),datarequestor+'/downloadedfiles/'+i),low_memory=False)
                subtempA=temp[temp.columns.intersection(set(['subject', 'redcap_event']+Evars2))]
                for j in ['site', 'study', 'PIN', 'redcap_event_name', 'site', 'study_id', 'id', 'gender','ethnic','age','racial','sex']+droplist:
                    try:
                        subtempA = subtempA.drop(columns=[j]).copy()
                    except:
                        pass
                if 'AABC' in i:
                    if "ASA24-Totals" in i:
                        subtempA=subtempA.drop_duplicates(subset=['subject','redcap_event'],keep='last')
                    widefileAABC=pd.merge(widefileAABC,subtempA, on=['subject','redcap_event'],how='outer')
                if 'HCA' in i:
                    widefileHCA = pd.merge(widefileHCA, subtempA, on=['subject', 'redcap_event'], how='outer')
            except:
                pass
#need to move Apoe
        if 'FAMILY' in i:
            print(i)
            temp = pd.read_csv(os.path.join(os.getcwd(),datarequestor+'/downloadedfiles/' + i), low_memory=False)
            subtempA = temp[temp.columns.intersection(set(['subject'] + Evars2))].copy()
            for j in ['age','sex','site','study', 'PIN', 'redcap_event_name', 'site', 'study_id', 'id', 'gender']+droplist:
                try:
                    subtempA = subtempA.drop(columns=[j]).copy()
                except:
                    pass
            widefileHCA=pd.merge(widefileHCA,subtempA,on='subject',how='outer')

#APOE and Metabolites coming in from a combined HCA/AABC source -- read and merge later
        if 'Metabolites' in i :
            print(i)
            try:
                temp=pd.read_csv(os.path.join(os.getcwd(),datarequestor+'/downloadedfiles/'+i),low_memory=False)
                mergelaterMet=temp[temp.columns.intersection(set(['subject', 'redcap_event']+Evars2))]
            except:
                print('fail')
        if 'APOE' in i.upper() :
            print(i)
            try:
                temp=pd.read_csv(os.path.join(os.getcwd(),datarequestor+'/downloadedfiles/'+i),low_memory=False)
                mergelaterAPOE=temp[temp.columns.intersection(set(['subject']+Evars2))]
            except:
                print('fail')


widefileAABC['study'] = 'AABC'
widefileHCA['study'] = 'HCA'
#drop oops from widefileHCA
print(widefileHCA.shape)
widefileHCA=widefileHCA.drop_duplicates(subset=['subject','redcap_event'])
print(widefileHCA.shape)
widefileHCA.redcap_event.value_counts()
widefileHCA=widefileHCA.loc[~((widefileHCA.subject.isin(v2oops)) & (widefileHCA.redcap_event.isin(['V2','F2','F3','Covid','CR'])))]
widefileHCA=widefileHCA.loc[~(widefileHCA.subject.isin(PCMPlist))]
print(widefileHCA.shape)
widefileHCA.redcap_event.value_counts()
HCAsubs=list(widefileHCA.loc[widefileHCA.IntraDB.isin(['CCF_HCA_STG','Behavioral Only'])].subject.unique())
widefileHCA.loc[widefileHCA.site=='UMinn','site']='UMN'
#drop the v2ooops for behavioral only second visits


print(widefileAABC.shape)
widefileAABC=widefileAABC.drop_duplicates(subset=['subject','redcap_event'])
print(widefileAABC.shape)

wide=pd.concat([widefileAABC,widefileHCA],axis=0).copy()
wide=wide.drop_duplicates(subset=['subject','redcap_event'],keep='first')

#merge in the 'mergelater' files (e.g. APOE and metabolites files that are produced for AABC and HCA combined)
if not mergelaterMet.empty:
    wide=pd.merge(wide,mergelaterMet,on=['subject','redcap_event','PIN'],how='left')
if not mergelaterAPOE.empty:
    wide=pd.merge(wide,mergelaterAPOE,on=['subject'],how='left')

#clean up age variable
wide.event_age=wide.event_age.round(1)

#don't subset by event...drop missing rows
wide['countmiss']=wide.isna().sum(axis=1)
#wide=wide.loc[wide.countmiss<(wide.shape[1]-14)]
wide.countmiss.value_counts()

#drop empty columns
wide.dropna(how='all', axis=1, inplace=True)
wide=wide.drop(columns=['countmiss'])
wide=wide.drop_duplicates().copy()
#drop the CCF_PCMP_ITK subjects
wide=wide.loc[~(wide.subject.isin(PCMPlist))].copy()
#make sure you're only getting subjects in the inventory
wide=wide.loc[wide.event_age>0]

harmony=wide.copy()
harmony['Cohort']=''
harmony['HCAAABC_event']=harmony.redcap_event
harmony.loc[((harmony.study=='AABC') & (harmony.redcap_event_name.str.contains('arm_1')))
         | ((harmony.study=='AABC') & (harmony.redcap_event_name.str.contains('arm_2')))
         | ((harmony.study=='AABC') & (harmony.redcap_event_name.str.contains('arm_3')))
         | ((harmony.study=='AABC') & (harmony.redcap_event_name.str.contains('arm_4'))),'Cohort']='AABC A'
harmony.loc[((harmony.study=='AABC') & (harmony.redcap_event_name.str.contains('arm_5')))
         | ((harmony.study=='AABC') & (harmony.redcap_event_name.str.contains('arm_6')))
         | ((harmony.study=='AABC') & (harmony.redcap_event_name.str.contains('arm_7')))
         | ((harmony.study=='AABC') & (harmony.redcap_event_name.str.contains('arm_8'))),'Cohort']='AABC B'
harmony.loc[((harmony.study=='AABC') & (harmony.redcap_event_name.str.contains('arm_9')))
         | ((harmony.study=='AABC') & (harmony.redcap_event_name.str.contains('arm_10')))
         | ((harmony.study=='AABC') & (harmony.redcap_event_name.str.contains('arm_11')))
         | ((harmony.study=='AABC') & (harmony.redcap_event_name.str.contains('arm_12'))),'Cohort']='AABC C'
harmony.loc[(harmony.study=='HCA'),'Cohort']='HCA Cross'
v2list=list(harmony.loc[(harmony.redcap_event=='V2') & (harmony.study=='HCA'),'subject'].unique())
harmony.loc[(harmony.study=='HCA') & (harmony.subject.isin(v2list)),'Cohort']='HCA Long'

#check
harmony.loc[(harmony.study=='HCA') & (harmony.redcap_event=='V1')].IntraDB.value_counts(dropna=False)
harmony.loc[(harmony.study=='HCA') & (harmony.redcap_event=='V2')].IntraDB.value_counts(dropna=False)

eventaabc=harmony.loc[(harmony.subject.isin(freeze1Subs)) & (harmony.study=='AABC')][['redcap_event']]
eventhca=harmony.loc[(harmony.subject.isin(HCAsubs)) & (harmony.study=='HCA')][['redcap_event']]
events=pd.concat([eventaabc,eventhca])

#assign the harmonized event, then double check order by age
harmony.loc[(harmony.Cohort=='AABC A') & (harmony.redcap_event=='AF1'),'HCAAABC_event']='V3F1'
harmony.loc[(harmony.Cohort=='AABC A') & (harmony.redcap_event=='AF2'),'HCAAABC_event']='V4F1'
harmony.loc[(harmony.Cohort=='AABC A') & (harmony.redcap_event=='AF3'),'HCAAABC_event']='V4F2'
harmony.loc[(harmony.Cohort=='AABC B') & (harmony.redcap_event=='AF1'),'HCAAABC_event']='V2F1'
harmony.loc[(harmony.Cohort=='AABC B') & (harmony.redcap_event=='AF2'),'HCAAABC_event']='V3F1'
harmony.loc[(harmony.Cohort=='AABC B') & (harmony.redcap_event=='AF3'),'HCAAABC_event']='V3F2'
harmony.loc[(harmony.Cohort=='AABC C') & (harmony.redcap_event=='AF1'),'HCAAABC_event']='V1F1'
harmony.loc[(harmony.Cohort=='AABC C') & (harmony.redcap_event=='AF2'),'HCAAABC_event']='V2F1'
harmony.loc[(harmony.Cohort=='AABC C') & (harmony.redcap_event=='AF3'),'HCAAABC_event']='V2F2'
harmony.loc[(harmony.redcap_event=='AFZ'),'HCAAABC_event']='VZ'
harmony.loc[(harmony.redcap_event=='AF0'),'HCAAABC_event']='V0'
harmony.loc[(harmony.Cohort=='HCA Cross') & (harmony.redcap_event=='F1'),'HCAAABC_event']='V1F1'
harmony.loc[(harmony.Cohort=='HCA Cross') & (harmony.redcap_event=='F2'),'HCAAABC_event']='V1F2'
harmony.loc[(harmony.Cohort=='HCA Cross') & (harmony.redcap_event=='F3'),'HCAAABC_event']='V1F3'
harmony=harmony.reset_index()
harmony=harmony.drop(columns='index')
#common protocol deviations
longsubs=list(harmony.loc[(harmony.Cohort=='HCA Long')].subject.unique())
harmonysub=harmony.loc[(harmony.redcap_event.isin(['V1','V2','V3','F1','F2','F3'])) & (harmony.Cohort=='HCA Long')][['subject','redcap_event','HCAAABC_event','event_age']]
harmonysub=harmonysub.sort_values(['subject','event_age']).copy()
for s in longsubs:
    harmonysub.loc[(harmonysub.subject==s) & (harmonysub.redcap_event.shift(1)=='V1') & (harmonysub.redcap_event.str.contains('F')),'HCAAABC_event']='V1F1'
    harmonysub.loc[(harmonysub.subject==s) & (harmonysub.HCAAABC_event.shift(1)=='V1F1') & (harmonysub.redcap_event.shift(2)=='V1') & (harmonysub.redcap_event.str.contains('F')),'HCAAABC_event']='V1F2'
    harmonysub.loc[(harmonysub.subject==s) & (harmonysub.HCAAABC_event.shift(1)=='V1F2') & (harmonysub.redcap_event.shift(3)=='V1') & (harmonysub.redcap_event.str.contains('F')),'HCAAABC_event']='V1F3'
    harmonysub.loc[(harmonysub.subject==s) & (harmonysub.redcap_event.shift(1)=='V2') & (harmonysub.redcap_event.str.contains('F')),'HCAAABC_event']='V2F1'
    harmonysub.loc[(harmonysub.subject==s) & (harmonysub.HCAAABC_event.shift(1)=='V2F1') & (harmonysub.redcap_event.shift(2)=='V2') & (harmonysub.redcap_event.str.contains('F')),'HCAAABC_event']='V2F2'
    harmonysub.loc[(harmonysub.subject==s) & (harmonysub.HCAAABC_event.shift(1)=='V2F2') & (harmonysub.redcap_event.shift(3)=='V2') &( harmonysub.redcap_event.str.contains('F')),'HCAAABC_event']='V2F3'

#update the harmonized HCAAABC_event field
harmony.update(harmonysub.loc[(harmonysub.redcap_event.isin(['F1','F2','F3'])) & (harmony.Cohort=='HCA Long')][['HCAAABC_event']])
harmony.loc[harmony.HCAAABC_event.isin(['V1F1','V1F2','V1F3','V2F1','V2F2','V0','VZ','Covid','CR','A']),'IntraDB']='Behavioral Only'

#drop the non-visit events with ages <36 (was experimental and/or protocol deviation for non-visit events)
harmony=harmony.loc[~(harmony.event_age <36)]


#create output
sliceout = harmony[harmony.isna().sum(axis=1).ne(harmony.shape[1]-3)] #subtracting subject and redcap from total number of missings
#reorder columns
#create the freeze flag
sliceout['AABC_Freeze1_Nov2023']=''
sliceout['HCA_Freeze1_Nov2023']=''
sliceout['Union_Freeze1_Nov2023']=''
sliceout.loc[(sliceout.subject.isin(freeze1Subs)) & (sliceout.study=='AABC'),'AABC_Freeze1_Nov2023']=1
sliceout.loc[(sliceout.subject.isin(HCAsubs)) & (sliceout.study=='HCA'),'HCA_Freeze1_Nov2023']=1
sliceout.loc[sliceout.AABC_Freeze1_Nov2023==1].redcap_event.value_counts()
sliceout.loc[sliceout.HCA_Freeze1_Nov2023==1].redcap_event.value_counts()
sliceout.loc[(sliceout.HCA_Freeze1_Nov2023==1) | (sliceout.AABC_Freeze1_Nov2023==1),'Union_Freeze1_Nov2023']=1
sliceout.loc[sliceout.Union_Freeze1_Nov2023==1].redcap_event.value_counts()
len(sliceout.loc[sliceout.Union_Freeze1_Nov2023==1].subject.unique())
len(sliceout.loc[sliceout.AABC_Freeze1_Nov2023==1].subject.unique())

sliceout.loc[sliceout.Union_Freeze1_Nov2023==1 & sliceout.IntraDB.isin(['CCF_HCA_STG','AABC_STG'])].redcap_event.value_counts()
len(sliceout.loc[sliceout.Union_Freeze1_Nov2023==1 & sliceout.IntraDB.isin(['CCF_HCA_STG','AABC_STG'])].subject.unique())

firstcols=['study','Cohort','subject','HCAAABC_event','PIN','event_age','race','ethnic_group','M/F','site','IntraDB','AABC_Freeze1_Nov2023','Union_Freeze1_Nov2023']
lastcols=[col for col in sliceout.columns if col not in firstcols and col not in droplist ]
sliceout=sliceout[firstcols+lastcols].drop(columns=['PIN','HCA_Freeze1_Nov2023','HCAAABC_event','IntraDB']).copy()
sliceout.to_csv(os.path.join(os.getcwd(),datarequestor+"/"+study+"_Slice_"+ date.today().strftime("%Y-%m-%d") + '.csv'),index=False)
UnionFreeze=sliceout.loc[(sliceout.Union_Freeze1_Nov2023==1) & (sliceout.redcap_event.isin(['V1','V2','V3']))]
UnionFreeze.to_csv(os.path.join(os.getcwd(),datarequestor+"/"+study+"_FreezeSlice_"+ date.today().strftime("%Y-%m-%d") + '.csv'),index=False)
print("HCA-AABC Totals: \n",sliceout.loc[(sliceout.redcap_event.isin(['V1','V2','V3','V4']))].redcap_event.value_counts())
print("AABC Freeze Totals: \n",sliceout.loc[(sliceout.AABC_Freeze1_Nov2023==1)  & (sliceout.redcap_event.isin(['V1','V2','V3','V4']))].redcap_event.value_counts())
#print("AABC Freeze with Metabolites: \n",sliceout.loc[(sliceout.AABC_Freeze1_Nov2023==1) & (sliceout.Metabolites==1) & (sliceout.redcap_event.isin(['V1','V2','V3','V4']))].redcap_event.value_counts())
#print("AABC Freeze with AD Biomarkers: \n",sliceout.loc[(sliceout.AABC_Freeze1_Nov2023==1) & (sliceout.AD_Biomarkers==1)].redcap_event.value_counts())

print("HCA-AABC Union Freeze Totals: \n",sliceout.loc[(sliceout.Union_Freeze1_Nov2023==1) & (sliceout.redcap_event.isin(['V1','V2','V3','V4']))].redcap_event.value_counts())
#print("HCA-AABC Union Freeze with Metabolites: \n",sliceout.loc[(sliceout.Union_Freeze1_Nov2023==1) & (sliceout.Metabolites==1) ].redcap_event.value_counts())
#print("HCA-AABC Union Freeze with AD Biomarkers: \n",sliceout.loc[(sliceout.Union_Freeze1_Nov2023==1) & (sliceout.AD_Biomarkers==1) ].redcap_event.value_counts())
len(sliceout.loc[(sliceout.Union_Freeze1_Nov2023==1)].subject.unique())
len(sliceout.loc[(sliceout.AABC_Freeze1_Nov2023==1)].subject.unique())

""" Special breakdown
    notvar=['study','Plate','redcap_event','Olink Sample ID',
     'Cohort',
     'subject',
     'HCAAABC_event',
     'event_age',
     'race',
     'ethnic_group',
     'M/F',
     'site',
     'IntraDB','Metabolites','AD_Biomarkers']
    varlist = [i for i in sliceout.columns if 'Flag' not in i and 'Freeze1' not in i and 'ID' not in i and i not in notvar]
    slicenumeric=sliceout.copy()
    slicenumeric[varlist]=slicenumeric[varlist].apply(pd.to_numeric,errors='coerce')
    N=slicenumeric.groupby('M/F')[varlist].count().transpose()
    M=slicenumeric.groupby('M/F')[varlist].mean(numeric_only=True).transpose().rename(columns={'F':'F mean','M':'M mean'})
    S=slicenumeric.groupby('M/F')[varlist].std().transpose().rename(columns={'F':'F SD','M':'M SD'})
    pd.concat([N,M,S],axis=1).to_csv(os.path.join(os.getcwd(),datarequestor+"/"+"Metabolites_x_Sex.csv"))
"""

slicevars=[i for i in list(sliceout.columns)]# if i not in ['redcap_event','subject','study']]

#headerE=Evars.loc[(Evars['Variable / Field Name'].isin(['subject','redcap_event'])) & (E['Form / Instrument']=='Completeness Inventory')]
Evars2=Evars.copy()
Evars['newname']=Evars['Variable / Field Name']
Evars.loc[Evars['NIH Toolbox Prefix in Slice'].isnull()==False,'newname']= Evars['NIH Toolbox Prefix in Slice']+'_'+Evars['Variable / Field Name']
Evars.loc[Evars['NIH Toolbox Prefix in Slice'].isnull() == False, 'newname']=Evars.newname.str.replace(' ', '_')
D=Evars.loc[Evars['newname'].isin(slicevars)]
D=D.drop(columns=['newname'])
D.to_csv(os.path.join(os.getcwd(),datarequestor+"/"+study+"_Slice_Dictionary_"+ date.today().strftime("%Y-%m-%d") + '.csv'),index=False)

#get the precise filenames downlaoded for the receipt
y = list(Evars['HCA Pre-Release File'].unique())+list(Evars['AABC Pre-Release File'].unique())
new=[]
for j in y:
    print(j)
    newlist=[]
    try:
        newlist=[j.replace("<date>.csv",'')]
    except:
        pass
    new=new+newlist
versionlist=[]
for i in os.listdir(os.path.join(os.getcwd(),datarequestor+"/downloadedfiles")):
    for n in new:
        if n.upper() in i.upper():
            versionlist=versionlist+[i]


#plots:

skip_plots=['subject','redcap_event','PIN','Actigraphy_Cobra','HCA_Freeze1_Nov2023']
plotlist=[vars for vars in list(sliceout.columns) if vars not in skip_plots]

if wantplots:
    if os.path.exists(os.path.join(os.getcwd(),datarequestor+"/plots")):
        pass
    else:
        os.mkdir(os.path.join(os.getcwd(),datarequestor+"/plots"))
    for i in plotlist:
        try:
            sliceout[i].hist()
            if i=='M/F':
                i='sex'
            plt.title(i)
            plt.savefig(os.path.join(os.getcwd(),datarequestor+"/plots/"+i))#, *, dpi='figure', format=None, metadata=None,
                    #bbox_inches=None, pad_inches=0.1,
                    #facecolor='auto', edgecolor='auto',
                   # backend=None, **kwargs
                   # )
            plt.show()
        except:
            pass


#write receipt
file_object = open(os.path.join(os.getcwd(),datarequestor+"/"+study+"_Data_Request_Receipt_"+date.today().strftime("%Y-%m-%d")+".txt"), "w")
print("***************************************************************",file=file_object)
print("",file=file_object)
print("Data Requested By:",datarequestor, file=file_object)
print("Request Fulfilled:",date.today().strftime("%Y-%m-%d"),file=file_object)
print("", file=file_object)
print("***************************************************************",file=file_object)
print("",file=file_object)
print("Instruments Requested:",file=file_object)
print("",file=file_object)
for i in InstRequest:
    print(i,file=file_object)
print("",file=file_object)
print("***************************************************************",file=file_object)
print("",file=file_object)
print("Derivatives Requested:",file=file_object)
print("",file=file_object)
for i in DerivativesRequested:
    print(i,file=file_object)
print("",file=file_object)
print("***************************************************************",file=file_object)
print("",file=file_object)
print("Bulk Data Requested:",file=file_object)
print("",file=file_object)
for i in BulkRequested:
    print(i,file=file_object)
    print("-- LETS TALK -- ",file=file_object)
print("",file=file_object)
print("***************************************************************",file=file_object)
print("",file=file_object)
print("Data Returned:",file=file_object)
print("",file=file_object)
shutil.copyfile(statsdir+statsfile,os.path.join(os.getcwd(),datarequestor+"/"+statsfile))
print("Most Recent Recruitment Report:",statsfile,file=file_object)
print("Slice:",study+"_Slice_"+ date.today().strftime("%Y-%m-%d") + '.csv',file=file_object)
print("Slice Dictionary:",study+"_Slice_Dictionary_"+ date.today().strftime("%Y-%m-%d") + '.csv',file=file_object)
print("Slice Univariate Descriptions:",study+"Slice_Univariate_Descriptions.txt",file=file_object)
print("Slice Univariate Plots:","/plots",file=file_object)
#point to new download location which is the same as the slice:
box = LifespanBox(cache=os.path.join(os.getcwd(),datarequestor))
if 'Cognition Factor Analysis' in DerivativesRequested:
    box.downloadFile(config['cogsHCAAABC'])
    box.downloadFile(1331283608435) #the corresponding readme
    print("Cognition Factor Analysis: ",os.path.basename(box.downloadFile(config['cogsHCAAABC'])),file=file_object)
if 'Cardiometabolic Index and Allostatic Load' in DerivativesRequested:
    box.downloadFile(config['cardiosHCA'])
    f=box.downloadFile(config['cardiosHCA'])
    os.rename(f,os.path.join(os.getcwd(),datarequestor,"HCA_Cardiometabolic_Essentials.xlsx"))
    print("Cardiometabolic Index and Allostatic Load: HCA_Cardiometabolic_Essentials.xlsx",file=file_object)
    box.downloadFile(1287112770879)  #the Readme
if 'Vasomotor Symptoms (Processed)' in DerivativesRequested:
    print("")
    print("NOT AVAILABLE: Processed Vasomotor Symptom Data", file=file_object)
if 'Imaging Derived Phenotypes' in DerivativesRequested:
    print("")
    print("Imaging Derived Phenotypes for HCA: https://wustl.box.com/s/kohbh1xvh93o35ztns1y8j9nwxw9twqi",file=file_object)

print("", file=file_object)
print("***************************************************************", file=file_object)
print("", file=file_object)
print("Slice created from the following files in the Pre-Release folder(s):",file=file_object)
print("",file=file_object)
for i in versionlist:
    print(i,file=file_object)
print("", file=file_object)
#print("Links:",file=file_object)
#print("", file=file_object)
#print("AABC Pre-Release Folder in Box: https://wustl.box.com/s/9gnrbyq7fybw0wtd82zfoagki2d5uky1", file=file_object)
#print("HCA Pre-Release Folder in Box: https://wustl.box.com/s/9gnrbyq7fybw0wtd82zfoagki2d5uky1", file=file_object)
#print("Encyclopedia: https://wustl.box.com/s/kr7lfj1finvcblr0ye0ls39gglo9xqbx", file=file_object)
#print("", file=file_object)
file_object.close()

distrib_object = open(os.getcwd()+"/"+datarequestor+"/"+study+"_Slice_Univariate_Descriptions.txt", "w")
print("Slice_Univariate_Descriptions:",file=distrib_object)
print("",file=distrib_object)
for i in [j for j in sliceout.columns if j not in skip_plots]:
    print("************", file=distrib_object)
    print("", file=distrib_object)
    if len(sliceout[i].unique()) >= 25:
        print("Continuous description "+i+":", file=distrib_object)
        print("", file=distrib_object)
        print(sliceout[i].describe(),file=distrib_object)
        print("", file=distrib_object)
    if len(sliceout[i].unique()) <25:
        print("Categorical description for variable "+i+":", file=distrib_object)
        print("", file=distrib_object)
        print(sliceout[i].value_counts(dropna=False), file=distrib_object)
        print("", file=distrib_object)
print("",file=distrib_object)
print("***************************************************************",file=distrib_object)
distrib_object.close()

##################################################################################################
##################################################################################################
#clean up
#remove the downloaddir:
shutil.rmtree(os.path.join(os.getcwd(),datarequestor+"/downloadedfiles"))
