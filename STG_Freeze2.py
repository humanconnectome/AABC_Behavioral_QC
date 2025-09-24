#prepares the wide.csv and VIV for Balsa, along with all the stuff that needs to reside in the STG folder for Freeze 2.
#remember, one subject dropped since Freeze 1.  Use the latest inventory
#which is the restricted version of the file produced by

# Consortium facing: https://wustl.box.com/s/hbfg13mntmtuk8h477laseejpgzohfoh
#then add the FU and covid data for each of these folks, using the same cutoffs.

from ccf.box import LifespanBox
import pandas as pd
import matplotlib.pyplot as plt
import os
import shutil
from datetime import date
from config import *
import numpy as np
from functions import *

config = LoadSettings()
secret=pd.read_csv(config['config_files']['secrets'])
intradb=pd.read_csv(config['config_files']['PCP'])
box = LifespanBox(cache="./tmp")

##########################################################################
# hardcoded stuff that isn't in the config, yet
outp="/Users/petralenzini/work/Behavioral/AABC/AABC_Behavioral_QC/AABC_Behavioral_QC/tmp/"
JohnF2='340446712733'
PRCfolder='340425049927'
STGfolder='340448906116'
F2list=pd.read_csv(box.downloadFile('1991384510169'),low_memory=False)[['Freeze', 'study', 'subject', 'redcap_event', 'M/F', 'Site', 'race',
       'ethnic_group', 'event_date', 'age_visit', 'age_bin']] #sheet_name="All Visits Union",usecols="A:F")
currentcogfolder='262328612623' # Nichols_2024-03-27  Nichols_CogFactorRegen_6May2024
HCARedcap='1993763298620'

##########################################################################
# Complete the following
datarequestor='FZ2'  # name of folder in which you want to generate the slice.
study="HCA-AABC" #or HCA
wantplots = False #Do you want caveman distributions of plots?
DownloadF = True #False #use if the stuff in PRC has been updated
#########################################################################################################
#CODEX
### NEED TO DROP THE L, X, and U before creating or harmonizing variables since there is redundancy, such as in 'event_date'
### NEED TO UPDATE THE CODEX
#E=BalsaDictionary = pd.read_excel(box.downloadFile(config['balsadict']))
E=pd.read_excel("AABC_HCA_CODEX_2025-09-14_newvarsFreeze2.xlsx",sheet_name="Union-Freeze1_AABC-Union-Union-")
CNew=['Order','VarOrder','Variable','Form','Options','FSH','FSA','HCA','AABC','Prefix','Description','See','Access','Balsa_var','Balsa_label','CategoryOrder','Category','Balsa_Access','ExtendedOrder','ORorder','DisplayOrder','DisplayName', 'VIV']
E.columns=CNew
E.loc[E.FSA.isnull()==True,'FSA']=''
E.loc[E.FSH.isnull()==True,'FSH']=''
################################################################################
################################### Done with user specs #####################

## first download from PRC, filter, and then upload to STG
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
    freezemain=pd.DataFrame(box.list_of_files([str(PRCfolder)])).transpose()
    #download
    for i in list(freezemain.fileid):
        if DownloadF:
            print("downloading",i,"...")
            box.downloadFile(i)
        else:
            print("already downloaded",i,"...")
    #also grab MRS data
    if DownloadF:
        box.downloadFile(str(config['mrsRatoi']))
    #get limited age
    if DownloadF:
        box2 = LifespanBox(cache=os.path.join(os.getcwd(), datarequestor + '/downloadedfilesSHUXLR/'))
        box2.downloadFile(str(config['unionfreeze1Lage']))
    else:
        print("already downloaded",str(config['unionfreeze1Lage']))
except:
    print("Something went wrong")

C=F2list.copy()
C['PIN']=C.subject+"_"+C.redcap_event
widefile=C[['PIN','subject','redcap_event']].copy()

allfiles=os.listdir(os.path.join(os.getcwd(),datarequestor+'/downloadedfiles/'))
#will pull in a special version of the scanner file at the very end that also includes site from the VIV.  Only the scanner variable was missing from the original freeze, but site was harmonized in completeness, which doesn't exist in the behavioarl and other data folder
listfiles=[i for i in allfiles if "Scanner" not in i]

## SSAGA has addendum vars so need to merge them together before uploading to STG:
Dssaga=pd.read_csv(os.path.join(os.getcwd(), datarequestor + '/downloadedfiles/', 'Freeze1_HCA_SSAGA_2024-08-20.csv'), low_memory=False)
print("before:",Dssaga.shape)
Dssagaadd=pd.read_csv(os.path.join(os.getcwd(), datarequestor + '/downloadedfiles/', 'Freeze1-add_HCA_SSAGA_2025-02-11.csv'), low_memory=False)
Dssaga=pd.merge(Dssaga,Dssagaadd,on=['subject','redcap_event'],how='left')
print("before:",Dssaga.shape)
print(str(len(Dssaga.subject.unique())))
Dupload=pd.merge(widefile,Dssaga,on=['subject','redcap_event'],how='inner')
print("after:",Dupload.shape)
print(str(len(Dssaga.subject.unique())))
Dupload.to_csv(os.path.join(outp,'Freeze2_HCA_SSAGA_'+ date.today().strftime("%Y-%m-%d") + '.csv'),index=False)
box.upload_file(os.path.join(outp,'Freeze2_HCA_SSAGA_'+ date.today().strftime("%Y-%m-%d") + '.csv'), STGfolder)

#AABC TLBX JUST needs to be filtered and renamed then uploaded to STG
TScore=pd.read_csv(os.path.join(os.getcwd(), datarequestor + '/downloadedfiles/', 'AABC_Upload_Processing_TLBX_Scores.csv'), low_memory=False)
print("before:",TScore.shape)
TScore2=pd.merge(widefile,TScore,on=['PIN'],how='inner')
print("after:",TScore.shape)
TScore2.to_csv(os.path.join(outp,'Freeze2_AABC_NIH-Toolbox-Scores_'+ date.today().strftime("%Y-%m-%d") + '.csv'),index=False)
box.upload_file(os.path.join(outp,'Freeze2_AABC_NIH-Toolbox-Scores_'+ date.today().strftime("%Y-%m-%d") + '.csv'), STGfolder)

TRaw=pd.read_csv(os.path.join(os.getcwd(), datarequestor + '/downloadedfiles/', 'AABC_Upload_Processing_TLBX_RAW.csv'), low_memory=False)
print("before:",TRaw.shape)
TRaw=pd.merge(widefile,TRaw,on=['PIN'],how='inner')
print("after:",TRaw.shape)
TRaw.to_csv(os.path.join(outp,'Freeze2_AABC_NIH-Toolbox-Raw_'+ date.today().strftime("%Y-%m-%d") + '.csv'),index=False)
box.upload_file(os.path.join(outp,'Freeze2_AABC_NIH-Toolbox-Raw_'+ date.today().strftime("%Y-%m-%d") + '.csv'), STGfolder)

# no changes to medications which were cleaned up in HarmHCAAABC *py program
box.upload_file(os.path.join(outp,"Freeze2_AABC-Clean-Medications_"+ date.today().strftime("%Y-%m-%d") + '.csv'), STGfolder)
box.upload_file(os.path.join(outp,"Freeze2_HCA-Clean-Medications_"+ date.today().strftime("%Y-%m-%d") + '.csv'), STGfolder)

# no changes to adbiomarkers or metabolites from PRC
box.upload_file(outp+'Union-Freeze2_AABC-HCA_AD-Biomarkers_'+ date.today().strftime("%Y-%m-%d") + '.csv', STGfolder)
box.upload_file(outp+'Union-Freeze2_AABC-HCA_Metabolites_'+ date.today().strftime("%Y-%m-%d") + '.csv', STGfolder)


MRS=pd.read_excel(box.downloadFile('1995948759844'), sheet_name='freeze 2')
MRS['subject']=MRS.ID.str.strip().str.split('_',1,expand=True)[0]
MRS['temp']=MRS.ID.str.split('_',1,expand=True)[1]
MRS['redcap_event']=MRS.temp.str.split('_',1,expand=True)[0]
MRS['PIN']=MRS['subject']+"_"+MRS['redcap_event']
#MRSdups1=MRS.loc[MRS.duplicated(subset='PIN',keep='first')].drop(columns=['ID','Age (years)','Gender','temp'])
#MRSdups2=MRS.loc[MRS.duplicated(subset='PIN',keep='last')].drop(columns=['ID','Age (years)','Gender','temp'])
MRS=MRS.drop_duplicates(subset=['PIN'],keep='last').drop(columns=['ID','Age (years)','Gender','temp'])
MRSQC=pd.merge(C[['PIN']],MRS,on='PIN',how='left') #MRS has a grifter
MRSQC.to_csv(os.path.join(outp,'Freeze2_AABC_MRS-conc-tCr_'+ date.today().strftime("%Y-%m-%d") + '.csv'),index=False)
box.upload_file(os.path.join(outp,'Freeze2_AABC_MRS-conc-tCr_'+ date.today().strftime("%Y-%m-%d") + '.csv'), STGfolder)

#need actigraphy and ASA

# Other HCA files just need to be filtered and renamed.
fixednonred_long=[i for i in listfiles if 'Penn' in i or "HCA_NIH" in i or "HCA_Q-Interactive" in i]
for i in fixednonred_long:
    print("file:",i)
    D=pd.read_csv(os.path.join(os.getcwd(), datarequestor + '/downloadedfiles/', i), low_memory=False)
    D['PIN'] = D['subject'] + "_" + D['redcap_event']
    print(str(len(D.subject.unique())))
    print("before:",D.shape)
    D=pd.merge(widefile,D,on=['subject','redcap_event'],how='inner')
    print("after:",D.shape)
    print(str(len(D.subject.unique())))
    D.to_csv(os.path.join(outp, i[:-14].replace("Freeze1", "Freeze2") + date.today().strftime("%Y-%m-%d") + '.csv'), index=False)
    box.upload_file(os.path.join(outp, i[:-14].replace("Freeze1", "Freeze2") + date.today().strftime("%Y-%m-%d") + '.csv'), STGfolder)

#Freeze1_HCA_NIH-Toolbox-Raw#
fixednonred_short=[i for i in listfiles if 'Apoe' in i or "Pedigree" in i]
for i in fixednonred_short:
    print("file:",i)
    D=pd.read_csv(os.path.join(os.getcwd(), datarequestor + '/downloadedfiles/', i), low_memory=False)
    print(str(len(D.subject.unique())))
    print("before:",D.shape)
    D=D.loc[~(D.subject=='HCA6539885')]
    print("after:",D.shape)
    print(str(len(D.subject.unique())))
    D.to_csv(os.path.join(outp, i[:-14].replace("Freeze1", "Freeze2") + date.today().strftime("%Y-%m-%d") + '.csv'), index=False)
    box.upload_file(os.path.join(outp, i[:-14].replace("Freeze1", "Freeze2") + date.today().strftime("%Y-%m-%d") + '.csv'), STGfolder)

#REDCaps need to be merged (drop checkbox vars) and uploaded (separate for AABC and HCA)
R1 = pd.read_csv(os.path.join(os.getcwd(), datarequestor + '/downloadedfiles/', 'AABC-RedCap_Processing_2025-09-23.csv'), low_memory=False)
R1.redcap_event.value_counts()
R1=R1[[i for i in R1.columns if "___" not in i]]
R1.to_csv(os.path.join(outp, 'Freeze2_AABC_RedCap_' + date.today().strftime("%Y-%m-%d") + '.csv'), index=False)
box.upload_file(os.path.join(outp, 'Freeze2_AABC_RedCap_' + date.today().strftime("%Y-%m-%d") + '.csv'), STGfolder)
H1=pd.read_csv(os.path.join(os.getcwd(), datarequestor + '/downloadedfiles/', 'HCA-RedCap_Processing_2025-09-22.csv'), low_memory=False)
H1=H1[[i for i in H1.columns if "___" not in i]]
H1.to_csv(os.path.join(outp, 'Freeze2_HCA_RedCap_' + date.today().strftime("%Y-%m-%d") + '.csv'), index=False)
box.upload_file(os.path.join(outp, 'Freeze2_HCA_RedCap_' + date.today().strftime("%Y-%m-%d") + '.csv'), STGfolder)
#already took care of sexorient and fu checkbox vars





#####################################################
# now create the wide file
# Merge non-toolbox visit specific stuff together
# merge REDCaps, (make sure includes addendum variables such as checkbox strings and iihand_edinburgh
# check that all  ___ variables affected have been replaced with checkbox strings
# SSAGA needs to be merged with HCA and then stacked with AABC
# merge in subject-specific stuff
# Stack, transpose, and merge TOOLBOX data.
# add calculated variables like BMI.
# calculate income_derived (or other name)
# calculate quarteryeartime0
# calculate daysfromtime0
# apply filter to med names -> med*_abc
# keep BALSA variables; be careful about checkbox vars
# THen stack studies together, incorporating exceptions for APOE, FAMILIES, and REgistration variables


#for the non redcap files, go one by one and drop all but O and R and rename on the fly
#note exception for cobra_meantst variable which needs to be kept around for the later manipulation
print(widefile.shape)
for i in listfiles:
    print("file:",i)
    #pd.DataFrame(listfiles)[0].str.split('_', expand=True)[2]
    #if 'Actigraphy' in i or 'Totals' in i or 'STRAW' in i or 'PennCNP' in i or 'Metabolites' in i or 'ADI' in i:
    for s in ['Actigraphy','Totals','STRAW','PennCNP','Metabolites','ADI']:
        if s in i:
            #print("processing...",s)
            D = pd.read_csv(os.path.join(os.getcwd(), datarequestor + '/downloadedfiles/', i), low_memory=False)
            check = [d for d in D.columns if d in widefile.columns and d not in ['subject', 'redcap_event', 'PIN']]
            D['PIN'] = D['subject'] + "_" + D['redcap_event']
            if len(check) > 0:
                print("NON-unique variables:", check)
            D = D.drop_duplicates(subset=['subject', 'redcap_event'])
            Es=E.loc[(~(E.Variable=='PIN')) & ((E.FSH.str.contains(s)) | (E.FSA.str.contains(s))) & ((E.Access.isin(['R','O'])) | (E.Balsa_var=='cobra_meantst'))][['Variable','Balsa_var']]
            #print(s,' Es is shape',Es.shape)
            #print(Es.head())
            Ereplace=dict(zip(Es.Variable,Es.Balsa_var))
            if s=='Totals':
                print(Ereplace)
            #print('before rename: ',D.shape)
            #print(D.columns)
            D=D.rename(columns=Ereplace)
            #print('after rename and BSHUXL drop: ',D.shape)
            colkeep=list(Es.Balsa_var)
            D=D[['subject','redcap_event','PIN']+colkeep].copy()
            #print(D.columns)
            widefile=widefile.merge(D,on=['subject','redcap_event','PIN'],how='left')
            print(i,widefile.shape)
    if 'MRS' in i:
        print("processing...MRS")
        D=pd.read_excel(os.path.join(os.getcwd(),datarequestor+'/downloadedfiles/',i),sheet_name='conc_tCr')
        check = [d for d in D.columns if d in widefile.columns and d not in ['subject', 'redcap_event', 'PIN']]
        D['PIN'] = D['subject'] +"_"+ D['redcap_event']
        print(i,D.shape)
        check = [d for d in D.columns if d in widefile.columns and d not in ['subject', 'redcap_event', 'PIN']]
        if len(check) > 0:
            print("NON-unique variables:", check)
        s='MRS'
        Es = E.loc[(~(E.Variable=='PIN')) & ((E.FSH.str.contains(s))  | (E.Variable.str.contains("diff_3TA_7T_days")) | (E.FSA.str.contains(s))) & (E.Access.isin(['R', 'O']))][
            ['Variable', 'Balsa_var']]
        print(s, ' Es is shape', Es.shape)
        print(Es.head())
        Ereplace = dict(zip(Es.Variable, Es.Balsa_var))
        print('before rename: ', D.shape)
        #print(D.columns)
        D = D.rename(columns=Ereplace)
        print('after rename and BSHUXL drop: ', D.shape)
        colkeep = list(Es.Balsa_var)
        D = D[['subject', 'redcap_event', 'PIN'] + colkeep].copy()
        #print(D.columns)
        widefile = widefile.merge(D, on=['subject', 'redcap_event','PIN'],how='left')
        print(i,widefile.shape)
    elif 'Apoe' in i:
        print("processing...Apoe")
        D=pd.read_csv(os.path.join(os.getcwd(),datarequestor+'/downloadedfiles/',i),low_memory=False)
        check = [d for d in D.columns if d in widefile.columns and d not in ['subject', 'redcap_event', 'PIN']]
        if len(check)>0:
            print("NON-unique variables:",check)
        s = 'Apoe'
        Es = E.loc[(~(E.Variable=='PIN')) & ((E.FSH.str.contains(s)) | (E.FSA.str.contains(s))) & (E.Access.isin(['R', 'O']))][
            ['Variable', 'Balsa_var']]
        print(s, ' Es is shape', Es.shape)
        print(Es.head())
        Ereplace = dict(zip(Es.Variable, Es.Balsa_var))
        print('before rename: ', D.shape)
        #print(D.columns)
        D = D.rename(columns=Ereplace)
        print('after rename and BSHUXL drop: ', D.shape)
        colkeep = list(Es.Balsa_var)
        D = D[['subject'] + colkeep].copy()
        #print(D.columns)
        widefile=widefile.merge(D, on=['subject'],how='left')
        print(i,widefile.shape)
    elif 'Pedigree' in i:
        print("processing...Pedigree")
        D=pd.read_csv(os.path.join(os.getcwd(),datarequestor+'/downloadedfiles/',i),low_memory=False)
        check = [d for d in D.columns if d in widefile.columns and d not in ['subject', 'redcap_event', 'PIN']]
        if len(check)>0:
            print("NON-unique variables:",check)
        s = 'Pedigree'
        Es = E.loc[((E.FSH.str.contains(s)) | (E.FSA.str.contains(s))) & (E.Access.isin(['R', 'O']))][['Form','Variable', 'Balsa_var','Access']]
        print(s, ' Es is shape',Es.shape)
        Es=Es.loc[~(Es.Variable=='pedid')]
        print(Es.head())
        Ereplace = dict(zip(Es.Variable, Es.Balsa_var))
        print('pedigree shape before rename: ', D.shape)
        #print(D.columns)
        D = D.rename(columns=Ereplace)
        print('pedigree shape after rename and BSHUXL drop: ', D.shape)
        D=D.drop_duplicates(subset='subject')
        D.loc[D.family_details.isnull()==True,'family_details']=''
        colkeep = list(Es.Balsa_var)
        D = D[['subject'] + colkeep].copy()
        print(D.columns)
        widefile=pd.merge(widefile,D, on=['subject'],how='left')
        print(i,widefile.shape)

widefilesansredcap=widefile.copy()
widefilebak=widefile.copy()
#redcap SHUXLR
#HCA
widefilemedslist=[i for i in widefile.columns if 'med' in i and 'sad' not in i]

LredH=fullredHCA.copy()

redHfile=[r for r in listfiles if 'RedCap' in r and 'HCA' in r]
colsmain=pd.read_csv(os.path.join(os.getcwd(),datarequestor+'/downloadedfiles/','Freeze1_HCA_RedCap_2024-08-20.csv'),low_memory=False).columns
getcols=[i for i in LredH.columns if i not in colsmain]

rh=LredH[['subject','redcap_event']+getcols]
# iihand_edinburgh will come in with the addendum
rh=rh.drop(columns=['iihand_edinburgh']).copy()
rhbak=rh.copy()

for i in redHfile:
    rh0=pd.read_csv(os.path.join(os.getcwd(),datarequestor+'/downloadedfiles/',i),low_memory=False)
    rh=pd.merge(rh,rh0,on=['subject','redcap_event'],how='inner')
checkbox=[i for i in rh.columns if '___' in i]
rh=rh.drop(columns=checkbox)

ss=[s for s in listfiles if 'SSAGA' in s]
sss=pd.DataFrame(columns=['subject','redcap_event'])
for i in ss:
    s0=pd.read_csv(os.path.join(os.getcwd(),datarequestor+'/downloadedfiles/',i),low_memory=False)
    print(s0.shape)
    sss=pd.merge(sss,s0,on=['subject','redcap_event'],how='outer')
    print(sss.shape)

rhh=pd.merge(rh,sss.drop(columns='PIN'),on=['redcap_event','subject'],how='left')
checkbox=[i for i in rhh.columns if '___' in i]
rhh=rhh.drop(columns=checkbox).copy()
#harmonize event_date
# map visits,ages, and event registrations.
rhh.loc[rhh.redcap_event == 'V1', 'event_date'] = rhh.v1_date
rhh.loc[rhh.redcap_event == 'V2', 'event_date'] = rhh.v2_date

###
LredA=fullredAABC.copy()
redAfile=[r for r in listfiles if 'RedCap' in r and 'AABC' in r]
colsmain=pd.read_csv(os.path.join(os.getcwd(),datarequestor+'/downloadedfiles/','Freeze1_AABC_RedCap_2024-08-20.csv'),low_memory=False).columns
getcols=[i for i in LredA.columns if i not in colsmain][2:]

ra=LredA[['subject','redcap_event']+getcols+AF0vars]
ra=ra.drop(columns=['iihand_edinburgh']).copy()

for i in redAfile:
    temp=pd.read_csv(os.path.join(os.getcwd(),datarequestor+'/downloadedfiles/',i),low_memory=False)
    for d in AF0vars:
        try:
            print('trying to drop',d)
            temp=temp.drop(columns=[d])
        except:
            print('failed to drop',d)
    ra=pd.merge(ra,temp,on=['subject','redcap_event'],how='inner')

print("potentially duplicated variables",[i for i in ra.columns if ('_y' in i or '_x' in i)])
checkbox=[i for i in ra.columns if '___' in i]
ra=ra.drop(columns=checkbox)
rabak=ra.copy()

#ALL TOGETHER
R=pd.concat([ra,rhh],axis=0).drop(columns=['PIN','study'])
Rmedlist=[i for i in R.columns if 'med' in i and 'sad' not in i and '_' not in i and 'care' not in i]
medcurate=pd.read_csv('/Users/petralenzini/work/Behavioral/AABC/AABC_Behavioral_QC/AABC_Behavioral_QC/medMapLifespanAABC_27Sept2024.map')
R=swapmeds(R, dict(zip(medcurate.upcaseTrimOriginal,medcurate.cleanMedRough1)), Rmedlist)
renamemeds=dict(zip(Rmedlist,[i+'_abc' for i in Rmedlist]))
R=R.rename(columns=renamemeds)

##Quick export of meds_abc
#med_add=['subject','redcap_event']+[i for i in R.columns if '_abc' in i]
#R[med_add].to_csv(outp+'Freeze1-add_AABC-HCA_Basic-Clean-Medications_2025-02-28.csv',index=False)
#R.shape

#subset, rename, and reorder vars according to Encyclopedia
harmonize=['height','bld_fasting','bld_core_fast','height_ft','height_in','cobra_meantst','bp_sitting_systolic','bp_sitting_diastolic','bp_standing_systolic','bp_standing_diastolic']
Es = E.loc[(~(E.Variable=='PIN')) &  ((E.Variable=='iihand_edinburgh') | (E.FSH.str.contains('RedCap')) | (E.FSH.str.contains('SSAGA')) | (E.FSA.str.contains('RedCap')) | (E.FSA.str.contains('SSAGA')) ) & ((E.Access.isin(['R', 'O'])) | (E.Variable=='event_date') | (E.Variable.isin(harmonize)))][['Variable', 'Balsa_var']]
print(Es.head())
Ereplace = dict(zip(Es.Variable, Es.Balsa_var))
print('before rename: ', R.shape)
#print(R.columns)
R = R.rename(columns=Ereplace)
print('after rename and BSHUXL drop: ', R.shape)

#don't forget the Rmedlist...these were derived variables
colkeep = list(Es.Balsa_var)+[i+'_abc' for i in Rmedlist]
R = R[['subject', 'redcap_event'] + colkeep].copy()

# Transcendent: APOE_Genotype, Pedigree, Race, Ethnicity, Sex, Study, etc.)
# Harmonized : , FASTING,height, daysfromV1, yearquarterV1, event_date
# Derived : RAVLT, BMI
# PPPP

#Rbak=R.copy()
#doing the rename for the completeness csv variables.
#Fix the MRS variable to reflect 138 vs 105
R=pd.merge(C,R,on=['subject','redcap_event'],how='inner')
Ecomplete = E.loc[(~(E.Variable=='PIN')) &  (E.Form.str.contains('Complete'))][['Variable', 'Balsa_var','Access']]
print(Ecomplete.head())
Ereplace = dict(zip(E.Variable, E.Balsa_var))
print('before rename: ', R.shape)
#print(R.columns)
R = R.rename(columns=Ereplace)
print('after rename and BSHUXL drop: ', R.shape)

R['event_date']=pd.to_datetime(R.event_date)
date1=R.sort_values(['subject','event_date'])[['subject','event_date']].drop_duplicates(subset=['subject'],keep='first').rename(columns={'event_date':'first_date'})
print(date1[['subject','first_date']].head())
R=pd.merge(R,date1,how='left',on='subject')
R['days_from_V1']=(R.event_date - R.first_date).dt.days
R['yearquarter_V1']=pd.to_datetime(R['first_date']) - pd.offsets.QuarterBegin(startingMonth=1)


R['PIN'] = R['subject'] + "_" + R['redcap_event']
widefileWredcap=pd.merge(widefile,R,on=['subject', 'redcap_event','PIN'],how='left')
print(widefile.shape)
print(widefileWredcap.shape)

#calculate BMI from height and weight for HCA
#note that height is harmonized from U variables
widefileWredcap.loc[widefileWredcap.height_ft.isnull(),'height_ft']=widefileWredcap.height.str.split("'",expand=True)[0]
widefileWredcap.loc[widefileWredcap.height_in.isnull(),'height_in']=widefileWredcap.height.str.split("'",expand=True)[1]
### add to data dictionary ###
widefileWredcap['height_inches']=12 * widefileWredcap.height_ft.astype(float) + widefileWredcap.height_in.astype(float)
widefileWredcap['bmi']=(widefileWredcap['weight']/(widefileWredcap['height_inches']*widefileWredcap['height_inches'])*703).round(2)

#fasting
widefileWredcap['blood_fasting']=''
widefileWredcap.loc[((widefileWredcap.bld_core_fast==1) | (widefileWredcap.bld_fasting==1)),'blood_fasting' ]=1
widefileWredcap.loc[((widefileWredcap.bld_core_fast==0) | (widefileWredcap.bld_fasting==0)),'blood_fasting' ]=0

#convert units on MEANTST (days) to hours MEANTST_HRS
widefileWredcap['cobra_meantst_hrs']=24*widefileWredcap['cobra_meantst']


# NEW
# actually don't add this variable.  Use edinburgh instead
#varlistnoeye=["iihandwr","iihandth","iihandsc","iihandto","iihandkn","iihandsp","iihandbr","iihandma","iihandbo","iihandfk"]
#widefileWredcap['handvarsum']=widefileWredcap[varlistnoeye].sum(axis=1,skipna=False)
#widefileWredcap.loc[widefileWredcap['handvarsum'].isnull()==True]
#NEW

#RAVLT
qfiles=[q for q in listfiles if 'Q-Interactive' in q]
Q=pd.concat([pd.read_csv(os.path.join(os.getcwd(),datarequestor+'/downloadedfiles/',qfiles[0])), pd.read_csv(os.path.join(os.getcwd(),datarequestor+'/downloadedfiles/',qfiles[1]))],axis=0)

widefileWredcapQ=pd.merge(widefileWredcap,Q,on=['subject','redcap_event','PIN'],how='left')
print(widefileWredcapQ.shape)
#add RAVLT calculations
#Immediate Recall
widefileWredcapQ.ravlt_pea_ravlt_sd_trial_i_tc=widefileWredcapQ.ravlt_pea_ravlt_sd_trial_i_tc.str.upper().str.replace("*NULL",'',regex=False)
widefileWredcapQ.ravlt_pea_ravlt_sd_trial_ii_tc=widefileWredcapQ.ravlt_pea_ravlt_sd_trial_ii_tc.str.upper().str.replace("*NULL",'',regex=False)
widefileWredcapQ.ravlt_pea_ravlt_sd_trial_iii_tc=widefileWredcapQ.ravlt_pea_ravlt_sd_trial_iii_tc.str.upper().str.replace("*NULL",'',regex=False)
widefileWredcapQ.ravlt_pea_ravlt_sd_trial_iv_tc=widefileWredcapQ.ravlt_pea_ravlt_sd_trial_iv_tc.str.upper().str.replace("*NULL",'',regex=False)
widefileWredcapQ.ravlt_pea_ravlt_sd_trial_v_tc=widefileWredcapQ.ravlt_pea_ravlt_sd_trial_v_tc.str.upper().str.replace("*NULL",'',regex=False)
widefileWredcapQ.ravlt_pea_ravlt_sd_trial_vi_tc=widefileWredcapQ.ravlt_pea_ravlt_sd_trial_vi_tc.str.upper().str.replace("*NULL",'',regex=False)
widefileWredcapQ.ravlt_pea_ravlt_sd_trial_i_tc=widefileWredcapQ.ravlt_pea_ravlt_sd_trial_i_tc.str.upper().str.replace("NULL*",'',regex=False)
widefileWredcapQ.ravlt_pea_ravlt_sd_trial_ii_tc=widefileWredcapQ.ravlt_pea_ravlt_sd_trial_ii_tc.str.upper().str.replace("NULL*",'',regex=False)
widefileWredcapQ.ravlt_pea_ravlt_sd_trial_iii_tc=widefileWredcapQ.ravlt_pea_ravlt_sd_trial_iii_tc.str.upper().str.replace("NULL*",'',regex=False)
widefileWredcapQ.ravlt_pea_ravlt_sd_trial_iv_tc=widefileWredcapQ.ravlt_pea_ravlt_sd_trial_iv_tc.str.upper().str.replace("NULL*",'',regex=False)
widefileWredcapQ.ravlt_pea_ravlt_sd_trial_v_tc=widefileWredcapQ.ravlt_pea_ravlt_sd_trial_v_tc.str.upper().str.replace("NULL*",'',regex=False)
widefileWredcapQ.ravlt_pea_ravlt_sd_trial_vi_tc=widefileWredcapQ.ravlt_pea_ravlt_sd_trial_vi_tc.str.upper().str.replace("NULL*",'',regex=False)
widefileWredcapQ.ravlt_pea_ravlt_sd_trial_i_tc = pd.to_numeric(widefileWredcapQ.ravlt_pea_ravlt_sd_trial_i_tc,errors='coerce')
widefileWredcapQ.ravlt_pea_ravlt_sd_trial_ii_tc = pd.to_numeric(widefileWredcapQ.ravlt_pea_ravlt_sd_trial_ii_tc,errors='coerce')
widefileWredcapQ.ravlt_pea_ravlt_sd_trial_iii_tc = pd.to_numeric(widefileWredcapQ.ravlt_pea_ravlt_sd_trial_iii_tc,errors='coerce')
widefileWredcapQ.ravlt_pea_ravlt_sd_trial_iv_tc = pd.to_numeric(widefileWredcapQ.ravlt_pea_ravlt_sd_trial_iv_tc,errors='coerce')
widefileWredcapQ.ravlt_pea_ravlt_sd_trial_v_tc = pd.to_numeric(widefileWredcapQ.ravlt_pea_ravlt_sd_trial_v_tc,errors='coerce')
widefileWredcapQ.ravlt_pea_ravlt_sd_trial_vi_tc = pd.to_numeric(widefileWredcapQ.ravlt_pea_ravlt_sd_trial_vi_tc,errors='coerce')

widefileWredcapQ['ravlt_immediate_recall']=widefileWredcapQ.ravlt_pea_ravlt_sd_trial_i_tc.astype(float)+widefileWredcapQ.ravlt_pea_ravlt_sd_trial_ii_tc.astype(float)+widefileWredcapQ.ravlt_pea_ravlt_sd_trial_iii_tc.astype(float)+widefileWredcapQ.ravlt_pea_ravlt_sd_trial_iv_tc.astype(float)+widefileWredcapQ.ravlt_pea_ravlt_sd_trial_v_tc.astype(float)
widefileWredcapQ['ravlt_learning_score']=widefileWredcapQ.ravlt_pea_ravlt_sd_trial_v_tc.astype(float)-widefileWredcapQ.ravlt_pea_ravlt_sd_trial_i_tc.astype(float)

Es = E.loc[(~(E.Variable=='PIN')) & ((E.FSH.str.contains('Q-Interactive')) | (E.FSH.str.contains('Q-Interactive'))) ][['Variable','Balsa_var']]
print('Es is shape', Es.shape)
print(Es.head())
Ereplace = dict(zip(Es.Variable, Es.Balsa_var))
print('before rename: ', widefileWredcapQ.shape)
#print(R.columns)
widefileWredcapQ = widefileWredcapQ.rename(columns=Ereplace)
print('after rename and BSHUXL drop: ', widefileWredcapQ.shape)
edrop = list(E.loc[((E.FSH.str.contains('Q-Interactive')) | (E.FSH.str.contains('Q-Interactive'))) & (~(E.Access.isin(['R', 'O']))) & (~(E.Variable.isin(['subject', 'redcap_event'])))]['Balsa_var'])
for e in edrop:
    try:
        widefileWredcapQ = widefileWredcapQ.drop(columns=[e]).copy()
        print('dropped',e)
    except:
        print('couldnt drop',e)


#Toolbox needs to be transposed.
Tfiles=[t for t in listfiles if 'NIH' in t and "Scores" in t]
t1=pd.read_csv(os.path.join(os.getcwd(),datarequestor+'/downloadedfiles/',Tfiles[0]),low_memory=False)
t2=pd.read_csv(os.path.join(os.getcwd(),datarequestor+'/downloadedfiles/',Tfiles[1]),low_memory=False)
T=pd.concat([t1,t2],axis=0)

#an aside to merge AABC toolbox registration files with freeze list
dfreg=pd.read_csv("/Users/petralenzini/work/Behavioral/AABC/AABC_Behavioral_QC/AABC_Behavioral_QC/temp3_TLBX_Registration.csv",low_memory=False)
dfregslim=dfreg[['PIN','MothersEducation']].drop_duplicates().copy()
Reg=pd.merge(C[['PIN','TLBX','study']].loc[(C.study=='AABC')],dfregslim,on='PIN',how='left')
Reg.loc[Reg.MothersEducation==999,'MothersEducation']=np.nan
Reg[['PIN','MothersEducation']].to_csv('/Users/petralenzini/work/Behavioral/AABC/AABC_Behavioral_QC/AABC_Behavioral_QC/Freeze1-AABC_NIH-Toolbox_Mothers-Education_2025Jun23.csv',index=False)


bulk = T.copy()
l = []
wide=C[['subject', 'redcap_event','PIN']]
tlbxlist = [j for j in list(E['Form'].unique()) if ('Covid' not in j) and ('Descriptions' not in j) and (('NIH' in j) or ('Cognition' in j and 'Composite' in j) or ('Summary (18+)' in j))]
for t in tlbxlist:
    l2 = [b for b in bulk.Inst.unique() if b[12:].replace('(18+)','') in b]
    l = l + l2
bulk = bulk.loc[bulk.Inst.isin(l)].copy()
if not bulk.empty:
    bulk['subject'] = bulk.PIN.str.split('_', expand=True)[0]
    bulk['redcap_event'] = bulk.PIN.str.split('_', expand=True)[1]
    for k in tlbxlist:
        print(k)
        tlbxvars = E.loc[((E['Form'] == k) | (E['Form']==k[12:]) | (E['Form'].str.upper()==k[12:].upper())) & (~E.Access.isin(['L','X','U']))][['Variable', 'Prefix', 'Access','Balsa_var']]
        tlbxvars['newname'] = tlbxvars['Prefix'] + '_' + tlbxvars['Variable']
        tlbxvars['newname'] = tlbxvars.newname.str.replace(' ', '_')
        mapvars = dict(
            zip(list(tlbxvars['Variable']), list(tlbxvars.newname)))  # ,tlbxvars['newname'])
        mapvars2 = dict(
            zip(list(tlbxvars['newname']), list(tlbxvars.Balsa_var)))  # ,tlbxvars['newname'])
        #if 'Negative Affect Summary' in k:
        #    k='Negative Affect Summary'
        #if 'Psychological Well Being' in k:
        #    k = 'Psychological Well Being'
        #if 'Social Satisfaction Summary' in k:
        #    k = 'Social Satisfaction Summary'
        temp = bulk.loc[bulk.Inst.str.upper().str.contains(k[12:].replace('(18+)','').upper())][['subject', 'redcap_event'] + list(tlbxvars['Variable'])]
        temp = temp.rename(columns=mapvars)
        temp = temp.rename(columns=mapvars2)
        for j in ['site', 'study', 'PIN', 'redcap_event_name', 'study_id', 'id', 'gender']:
            try:
                temp = temp.drop(columns=[j]).copy()
            except:
                pass
        temp=temp.drop_duplicates(subset=['subject','redcap_event'])
        wide = pd.merge(wide, temp, on=['subject', 'redcap_event'], how='outer')

#widefilebak2=widefile.copy()
#widefile=widefilebak2.copy()

widefile=pd.merge(widefileWredcapQ,wide,on=['subject','redcap_event'],how='left')
print(widefile.shape)
#DONT clean up age variable in internal freeze - not good for cog factor consistency.  Better to make note in Known issues and FAQ
#Do clean it up for Balsa release.
widefile.event_age=widefile.event_age.apply(np.floor)
widefile['age_open']=widefile.event_age.apply(np.floor)
widefile.loc[widefile.age_open > 90,'age_open']=90

widefile.loc[(widefile.bp_sitting.isnull()==True) & (widefile.bp_sitting_systolic.isnull()==False)][['study','bp_sitting','bp_standing','bp_sitting_systolic','bp_sitting_diastolic','bp_standing_systolic','bp_standing_diastolic']]
widefile.loc[(widefile.bp_sitting.isnull()==True) & (widefile.bp_sitting_systolic.isnull()==False),'bp_sitting']=widefile['bp_sitting_systolic'].round().astype('Int64').astype(str).replace('<NA>', '')+"/"+widefile['bp_sitting_diastolic'].round().astype('Int64').astype(str).replace('<NA>', '')
widefile.loc[(widefile.bp_standing.isnull()==True) & (widefile.bp_standing_systolic.isnull()==False),'bp_standing']=widefile['bp_standing_systolic'].round().astype('Int64').astype(str).replace('<NA>', '')+"/"+widefile['bp_standing_diastolic'].round().astype('Int64').astype(str).replace('<NA>', '')

#merge in cognitive factors
COGFACTORSALL=pd.DataFrame(columns=['subject','redcap_event'])
for f in ['Tr35-60y','Tr35-80y','Tr60-90y']:
    coglist = pd.DataFrame(box.list_of_files([str(currentcogfolder)])).transpose()
    cogf=coglist.loc[coglist.filename.str.contains(f)].reset_index()['index'][0]
    #cf=cogf.reset_index()['index'][0]
    facts=pd.read_csv(box.downloadFile(cogf), low_memory=False, encoding='ISO-8859-1')[['subject','Visit','Memory','FluidIQ','CrystIQ']]
    facts.columns=['subject','redcap_event','Memory_'+f.replace('-','_'),'FluidIQ_'+f.replace('-','_'),'CrystIQ_'+f.replace('-','_')]
    print(facts.shape)
    COGFACTORSALL=pd.merge(COGFACTORSALL,facts,on=['subject','redcap_event'],how='outer')

widefile=pd.merge(widefile,COGFACTORSALL,on=['subject','redcap_event'],how='left')

#missing vars
sitescan=pd.read_csv(os.path.join(os.getcwd(), datarequestor + '/downloadedfilesSHUXLR/','Freeze1_AABC-HCA_SiteScanner_2025-02-24.csv'))
widefile=pd.merge(widefile,sitescan,on='PIN',how='left')

#don't subset by event...drop missing rows
#note that this check shouldn't actually be doing anything
widefile['countmiss']=widefile.isna().sum(axis=1)
widefile.countmiss.value_counts()
widefile=widefile.loc[~(widefile.event_age <36)]
print(widefile.shape)

#group by study for missingness counts.
study_groups = widefile.groupby('study')
# Function to count missing and non-missing values
def count_missing(df):
    missing = df.isna().sum()
    nonmissing = df.shape[0] - missing
    return pd.DataFrame({'number_missing': missing, 'nonmissing': nonmissing})
# Apply function to each group
counts = {study: count_missing(group) for study, group in study_groups}
# Combine results into a single DataFrame and flatten multiIndex
result = pd.concat(counts, axis=1)
result.columns = [f'{col}_{study}' for study, col in result.columns]
result['number_missing']=result['number_missing_AABC']+result['number_missing_HCA']
result['nonmissing']=2247-result['number_missing']

result.to_csv(outp+'ColCounts.csv')


#emptycols=pd.DataFrame(widefile.isna().sum(axis=0),columns=['number_missing'])
#emptycols['nonmissing']=2247-emptycols.number_missing
#emptycols.to_csv(outp+'EmptyCols.csv')

widefilelastbak=widefile.copy()
#widefile=widefilelastbak.copy()

#finalvars=E.loc[E.Access.isin(['O','R'])][['Balsa_var','VarOrder','CatOrder']]
finalvars=pd.read_excel(box.downloadFile(John))



finalkeep=list(finalvars['BALSA Variable'])

check1=[i for i in finalkeep if i not in widefile.columns]
check2=[i for i in widefile.columns if i not in finalkeep]

widefile=widefile.rename(columns={'PIN':'id_event'})

widefile=widefile[finalkeep]

#lastminutecleanup
    #=str(widefile.bp_sitting_systolic.astype(int))+"/"+str(widefile.bp_sitting_diastolic.astype(int))
#[['study','bp_sitting','bp_standing','bp_sitting_systolic','bp_sitting_diastolic','bp_standing_systolic','bp_standing_diastolic']]
#addendum update to MRS counts:
widefile.loc[widefile.mrs_days_from_3T.isnull()==False,'MRS_7T']='YES'

widefile.bp_sitting=widefile.bp_sitting.str.replace('_','')
widefile.bp_standing=widefile.bp_standing.str.replace('_','')
widefile.loc[widefile.dm17b.isin(['-99999','-999']),'dm17b']='-9999'

widefile.loc[(widefile.Bulk_Imaging=='YES') & (widefile.MR_QC_Issue_Codes.isnull()==True),'MR_QC_Issue_Codes']='None'

for i in ['tMRI_CARIT_PctCompl','tMRI_FACENAME_PctCompl','tMRI_VISMOTOR_PctCompl']:
    widefile.loc[(widefile.Bulk_Imaging == 'YES') & (widefile[i].isnull() == True), i] = '0%'

widefile.loc[widefile.med_yn==2,'med_num']=0



#widefile['tics_score2'] = pd.to_numeric(widefile['tics_score'], errors='coerce')
## Fill NaNs with a default value (e.g., 0) and convert to int
#widefile['tics_score2'] = widefile['tics_score2'].fillna('').astype(int)

widefile['tics_score'] = widefile['tics_score'].replace('', np.nan)
widefile['tics_score'] = pd.to_numeric(widefile['tics_score'], errors='coerce')
widefile['croms_educ']=pd.to_numeric(widefile['croms_educ'], errors='coerce')

for i in widefile.columns[widefile.isin(['YES']).any()]:
    print('***** orig:')
    print(widefile[i].value_counts())
    widefile.loc[widefile[i]!='YES',i]='NO'
    print('with Nos')
    print(widefile[i].value_counts())

widefile.to_csv("AABC-HCA_Release1_BALSA_data_2025-04-14.csv", index=False)
vlist=list(finalvars.loc[finalvars['BALSA VIV'].astype('str').str.contains('V')]['BALSA Variable'])
widefile[vlist].to_csv("AABC-HCA_Release1_BALSA_VIV_2025-04-14.csv", index=False)

#plots:
skip_plots=['id_event','subject','redcap_event','PIN','Actigraphy_Cobra','HCA_Freeze1_Nov2023','COMMENTS','Notes','pseudo_guid','study']
vlabel=list(finalvars.loc[finalvars['BALSA VIV'].astype('str').str.contains('V')]['BALSA Display Name'])
  label_dict=dict(zip(vlist,vlabel))
label_dict = {key: re.sub(r'\s*\([^)]*\)', '', value) for key, value in label_dict.items()}

plotlist=[vars for vars in vlist if vars not in skip_plots]
if wantplots:
    if os.path.exists(os.path.join(os.getcwd(),datarequestor+"/plots")):
        pass
    else:
        os.mkdir(os.path.join(os.getcwd(),datarequestor+"/plots"))
    for i in plotlist:
        try:
            widefilesub = widefile.loc[~(widefile[i].isnull() == True)]
            widefilesub = widefilesub.loc[~(widefilesub[i] == '')]
            widefilesub[i].astype(float).hist()
            widefilesub[i].hist()
            plt.xlabel(label_dict.get(i))
            plt.ylabel('count')
            plt.savefig(os.path.join(os.getcwd(),datarequestor+"/plots/"+i))#, *, dpi='figure', format=None, metadata=None,
            plt.show()
        except:
            try:
                widefilesub = widefile.loc[~(widefile[i].astype(str).str.contains('<'))]
                widefilesub = widefilesub.loc[~(widefilesub[i].astype(str).str.contains('QNS'))]
                widefilesub = widefilesub.loc[~(widefilesub[i].astype(str).str.contains('>'))]
                widefilesub = widefilesub.loc[~(widefilesub[i].astype(str).str.contains('N/AA'))]
                widefilesub = widefilesub.loc[~(widefilesub[i].astype(str).str.contains('ND'))]
                widefilesub=widefilesub.loc[~(widefilesub[i].isnull()==True)]
                widefilesub=widefilesub.loc[~(widefilesub[i]=='')]
                widefilesub[i].astype(float).hist()
                plt.xlabel(label_dict.get(i))
                plt.ylabel('count')
                plt.savefig(os.path.join(os.getcwd(),
                                         datarequestor + "/plots/" + i))  # , *, dpi='figure', format=None, metadata=None,
                plt.show()
            except:
                try:
                    widefilesub[i].hist()
                    plt.xlabel(label_dict.get(i))
                    plt.ylabel('count')
                    plt.savefig(os.path.join(os.getcwd(),
                                             datarequestor + "/plots/" + i))  # , *, dpi='figure', format=None, metadata=None,
                    plt.show()
                except:
                    print('couldnt plot '+label_dict.get(i))
                    pass

