#many of the PRC files were just transferred over from Freeze 1 (HCA files)
#The AABC Toolbox data was sent to PRC from QC_datatypes


from ccf.box import LifespanBox
import pandas as pd
import matplotlib.pyplot as plt
import os
import shutil
from datetime import date
from config import *
import numpy as np
import json
from functions import *
import numpy as np
from pandas.api import types as pdt

config = LoadSettings()
secret=pd.read_csv(config['config_files']['secrets'])
intradb=pd.read_csv(config['config_files']['PCP'])
box = LifespanBox(cache="./tmp")

##########################################################################
# hardcoded stuff that isn't in the config, yet
medmap="/Users/petralenzini/work/Behavioral/AABC/AABC_Behavioral_QC/AABC_Behavioral_QC/medMapLifespanAABC_2b.map"
medmap1=pd.read_csv(medmap,sep=':',quotechar="'",header=None)
medmap1.columns=['old','new']
meddict=medmap1.set_index('old')['new'].to_dict()
medcat="/Users/petralenzini/work/Behavioral/AABC/AABC_Behavioral_QC/AABC_Behavioral_QC/medCats.json" #a dictionary

outp="/Users/petralenzini/work/Behavioral/AABC/AABC_Behavioral_QC/AABC_Behavioral_QC/tmp/"
JohnF2='340446712733'
PRCfolder='340425049927'
STGfolder='340448906116'
F2list=pd.read_csv(box.downloadFile('1991384510169'),low_memory=False)[['Freeze', 'study', 'subject', 'redcap_event', 'M/F', 'Site', 'race',
       'ethnic_group', 'event_date', 'age_visit', 'age_bin']] #sheet_name="All Visits Union",usecols="A:F")
currentcogfolder='262328612623' # Nichols_2024-03-27  Nichols_CogFactorRegen_6May2024

##########################################
# prep metabolites
#Metabolites  (reimporting in case there were any that were left out of freeze 1 due to cutoff)
Mheader=pd.read_excel(box.downloadFile(config['Metabolites']),sheet_name='raw',header=5,nrows=0)
Mbody=pd.read_excel(box.downloadFile(config['Metabolites']),sheet_name='raw',header=10)
cols=['subject','redcap_event']+list(Mbody.iloc[:,2:4].columns)+list(Mheader.iloc[:,4:].columns)
Mbody.columns=cols
Mbody.loc[Mbody.redcap_event==1,'redcap_event']='V1'
Mbody.loc[Mbody.redcap_event==2,'redcap_event']='V2'
Mbody.subject=Mbody.subject.str.upper().str.replace('_V1','')
Mbody.subject=Mbody.subject.str.upper().str.replace('_V2','')
Mplates=Mbody.loc[Mbody['Olink Sample ID']=='Plate LOD'].iloc[:,3:]
Mvalues=Mbody.loc[Mbody.subject.astype(str).str.contains('HCA')].copy()

#create your flag variables based in plate number and metabolite.
#loop for metabolites
mets=[k for k in Mplates.columns if "Plate" not in k]
for i in mets:
    Mvalues[i+"_Detection_Flags"]=''
    print(i)
    #set nan thresholds to zero since no flags for Nans - also, LLOQ for these columns is orders of magnitude smaller than column values, so threshold probably non-issue
    Mplates.loc[Mplates[i] == '> ULOQ',i]=9999999
    Mplates.loc[Mplates[i].isnull() == True,i]=0
    #loop for the plate for that particular metabolite
    for plate in [float(p) for p in [*range(1, 14, 1)]]:
        #find the threshold for that plate
        thresh=Mplates.loc[Mplates.Plate==plate,i].values[0]
        print("Plate",plate,i,"Threshold:",thresh)
        try:
            Mvalues.loc[(Mvalues[i]<thresh) & (Mvalues.Plate==plate),i+"_Detection_Flags"]="<Plate LOD"
        except:
            print("Trouble with",i)
        Mplates.loc[Mplates[i] == 9999999,i]='> ULOQ'
Mvalues['PIN']=Mvalues.subject+"_"+Mvalues.redcap_event

reordereda=['subject','redcap_event','PIN','Olink Sample ID', 'Plate']
a=[]
for i in mets:
    a = a + [i, i + "_Detection_Flags"]

Mvalues=Mvalues[reordereda+a].copy()
Mvalues['Metabolites']=1
#find vars with <80% call rate and set variable to U in encyclopedia

#check for duplicates
a=len(Mvalues.PIN.unique())

#find plates with met callrates < .8
plates_callrate=pd.DataFrame(columns=['M','plate'])
for i in range(1,14):
    print(i)
    platevals=Mvalues[mets].loc[Mvalues.Plate==int(i)]
    plate_missing = pd.DataFrame((platevals.isna().sum()/40)).reset_index()
    plate_missing.columns=['M','dropit']
    plate_missing['plate']=i
    plates_callrate=pd.concat([plates_callrate,plate_missing.loc[plate_missing.dropit>0.2][['M','plate']]])

#set values for this met to missing
for index,row in plates_callrate.iterrows():
    mm=row['M']
    pp=float(row['plate'])
    print(mm,pp)
    Mvalues.loc[Mvalues.Plate==pp,mm]=np.nan
    Mvalues.loc[Mvalues.Plate == pp, mm+"_Detection_Flags"] = "Plate CR < 80%"

Mvalues['PIN']=Mvalues.subject+"_"+Mvalues.redcap_event

MvaluesQC.shape
MvaluesQC=pd.merge(C[['Freeze','PIN']],Mvalues,on='PIN',how='outer',indicator=True)
MvaluesQC=MvaluesQC.loc[~(MvaluesQC._merge=='right_only')] #this is the grifter
MvaluesQC.shape

MvaluesQC.loc[MvaluesQC.duplicated(subset=['PIN'])]

MvaluesQC.drop(columns='_merge').to_csv(outp+'Union-Freeze2_AABC-HCA_Metabolites_'+ date.today().strftime("%Y-%m-%d") + '.csv',index=False)
box.upload_file(os.path.join(outp,'Union-Freeze2_AABC-HCA_Metabolites_'+ date.today().strftime("%Y-%m-%d") + '.csv'), PRCfolder)

#AD 2024 biomarkers (reimporting in case there were any that were left out of freeze 1 due to cutoff
AD=pd.read_excel(box.downloadFile(config['AD_Biom']),sheet_name='AD Biomarkers, MSD S-plex')
marks=[i for i in AD.columns if "Sample" not in i]
AD['PIN']=AD[' Sample ID_Visit'].str.upper()
AD['subject']=AD.PIN.str.split('_',1,expand=True)[0]
AD['redcap_event']=AD.PIN.str.split('_',1,expand=True)[1]
#AD.loc[AD.duplicated(subset=['PIN'])]

#first duplicate for HCA9178894 v2 has a strikethrough.  keeping second
AD=AD.drop_duplicates(subset=['PIN'],keep='last')[['subject','redcap_event','PIN']+marks]

#rename columns
newcols=[i.replace("  "," ").replace(" ","_").replace("/","_") for i in AD.columns]
AD.columns=newcols

#2025 AD_Biomarkers
NewBiomarkers='1995902207660'
ADNew=pd.read_excel(box.downloadFile(NewBiomarkers),sheet_name='Sheet1')

marks=[i for i in ADNew.columns if "Sample" not in i]
ADNew['PIN']=ADNew['Visit'].str.upper().str.replace('-','_')
ADNew['subject']=ADNew.PIN.str.split('_',1,expand=True)[0]
ADNew['redcap_event']=ADNew.PIN.str.split('_',1,expand=True)[1]
ADNew.loc[ADNew.duplicated(subset=['PIN'])]

#rename columns
newcols=[i.replace("  "," ").replace(" ","_").replace("/","_") for i in ADNew.columns]
ADNew.columns=newcols
ADNew=ADNew.drop(columns=['SampleID', 'Visit'])

ADAll=pd.concat([AD,ADNew],axis=0)
keepcols=[i for i in ADAll.columns if '%' not in i and 'Duplicate' not in i and 'Detection' not in i and 'Run' not in i]
ADAll.loc[ADAll.duplicated(subset=['PIN'],keep='first')][['pT217_Conc_pg_ml',  'GFAP_Conc_pg_ml','NfL_Conc_pg_ml', 'tTau_Conc_pg_ml']]
ADAll.loc[ADAll.duplicated(subset=['PIN'],keep='last')][['pT217_Conc_pg_ml',  'GFAP_Conc_pg_ml','NfL_Conc_pg_ml', 'tTau_Conc_pg_ml']]

QC=pd.merge(C[['Freeze','PIN']],ADAll[['PIN','pT217_Conc_pg_ml',  'GFAP_Conc_pg_ml','NfL_Conc_pg_ml', 'tTau_Conc_pg_ml']],on='PIN',how='outer',indicator=True)
QCnogrift=QC.loc[~(QC._merge=='right_only')] #this is the grifter
QCnogrift.shape
#keep last duplicate
QCnogrift=QCnogrift.drop_duplicates(subset='PIN',keep='last').copy()
QCnogrift.shape

QCnogrift.drop(columns='_merge').to_csv(outp+'Union-Freeze2_AABC-HCA_AD-Biomarkers_'+ date.today().strftime("%Y-%m-%d") + '.csv',index=False)
box.upload_file(outp+'Union-Freeze2_AABC-HCA_AD-Biomarkers_'+ date.today().strftime("%Y-%m-%d") + '.csv', PRCfolder)



##########################################

#PREPARE FULL HCA REDCAP WITH SUBJECT, REDCAP EVENT, and PIN (no filters for events or subjects except blanks)
#get full redcap data from redcap.wustl.edu (for variables that didn't make it to the freeze for whatever reason
fullredHCA=getfullRedExport('hcpa',secret)
REDCapHCA=fullredHCA.copy()
REDCapHCA['redcap_event']=REDCapHCA.replace({'redcap_event_name':
                                           {'visit_1_arm_1':'V1', 'follow_up_1_arm_1':'F1','visit_2_arm_1':'V2',  'follow_up_2_arm_1':'F2',
                                            'covid_arm_1':'Covid', 'follow_up_3_arm_1':'F3', 'covid_remote_arm_1':'CR',  'actigraphy_arm_1':'A',
                                           }})['redcap_event_name']
subs=REDCapHCA.loc[~((REDCapHCA.subject_id.isnull()==True) | (REDCapHCA.subject_id==""))][['id','subject_id','dob']].drop_duplicates()
subs=subs.rename(columns={'subject_id':'subject'})
REDCapHCA=pd.merge(REDCapHCA.drop(columns=['dob']),subs,how='left',on='id')
#get dates and ages
# map visits,ages, and event registrations (requires sub-event for Covid surveys ne CR).
HCAdf1=REDCapHCA.copy()
HCAdf1['sub_event'] = HCAdf1['redcap_event']
HCAdf1['sub_event_date'] = ''
HCAdf1['sub_event_age'] = ''
HCAdf1['sub_event_register'] = ''
HCAdf1.loc[HCAdf1.redcap_event == 'V1', 'event_date'] = HCAdf1.v1_date
HCAdf1.loc[HCAdf1.redcap_event == 'V1', 'event_age'] = HCAdf1.age
HCAdf1.loc[HCAdf1.redcap_event == 'V1', 'event_register'] = HCAdf1.visit
HCAdf1.loc[HCAdf1.redcap_event == 'V1', 'sub_event'] = "1.V1"
HCAdf1.loc[HCAdf1.redcap_event == 'F1', 'event_date'] = HCAdf1.v3_date
HCAdf1.loc[HCAdf1.redcap_event == 'F1', 'event_age'] = HCAdf1.age_v3
HCAdf1.loc[HCAdf1.redcap_event == 'F1', 'event_register'] = HCAdf1.visit3
HCAdf1.loc[HCAdf1.redcap_event == 'F1', 'sub_event'] = "2.F1"
HCAdf1.loc[HCAdf1.redcap_event == 'V2', 'event_date'] = HCAdf1.v2_date
HCAdf1.loc[HCAdf1.redcap_event == 'V2', 'event_age'] = HCAdf1.age_v2
HCAdf1.loc[HCAdf1.redcap_event == 'V2', 'event_register'] = HCAdf1.visit2
HCAdf1.loc[HCAdf1.redcap_event == 'V2', 'sub_event'] = "3.V2"
HCAdf1.loc[HCAdf1.redcap_event == 'F2', 'event_date'] = HCAdf1.v3_date  # yes this is right.  FU events are longitudinally tracked
HCAdf1.loc[HCAdf1.redcap_event == 'F2', 'event_age'] = HCAdf1.age_v3  # yes this is right.  FU events are longitudinally tracked
HCAdf1.loc[HCAdf1.redcap_event == 'F2', 'event_register'] = HCAdf1.visit4  # yes this is right.  Event registers consider F2 to be visit4
HCAdf1.loc[HCAdf1.redcap_event == 'F2', 'sub_event'] = "4.F2"
HCAdf1.loc[HCAdf1.redcap_event == 'F3', 'event_date'] = HCAdf1.v3_date
HCAdf1.loc[HCAdf1.redcap_event == 'F3', 'event_age'] = HCAdf1.age_v3
HCAdf1.loc[HCAdf1.redcap_event == 'F3', 'event_register'] = HCAdf1.visit5
HCAdf1.loc[HCAdf1.redcap_event == 'F3', 'sub_event'] = "5.F3"
HCAdf1.loc[HCAdf1.redcap_event == 'A', 'event_date'] = HCAdf1.v6_startdate
HCAdf1.loc[HCAdf1.redcap_event == 'A', 'event_age'] = HCAdf1.age_v6
HCAdf1.loc[HCAdf1.redcap_event == 'A', 'event_register'] = HCAdf1.visit6
HCAdf1.loc[HCAdf1.redcap_event == 'A', 'sub_event'] = "6.A"
# Covid
HCAdf1.loc[HCAdf1.redcap_event == 'Covid', 'event_date'] = HCAdf1.covid_dt
HCAdf1.loc[HCAdf1.redcap_event == 'Covid', 'event_register'] = HCAdf1.visit7
HCAdf1.loc[HCAdf1.redcap_event == 'Covid', 'sub_event'] = '7.Covid1'
# Covid2
Covid2 = HCAdf1.loc[HCAdf1.visit8.str.contains('8')].copy()
Covid2.event_date = Covid2.bt_covid_dt
Covid2.event_register = Covid2.visit8
Covid2.sub_event = '8.Covid2'
print(Covid2.shape)
# Covid Remote
HCAdf1.loc[HCAdf1.redcap_event == 'CR', 'event_date'] = HCAdf1.rt_covid_dt
HCAdf1.loc[HCAdf1.redcap_event == 'CR', 'event_register'] = HCAdf1.visit9
HCAdf1.loc[HCAdf1.redcap_event == 'CR', 'sub_event'] = '9.Covid Remote'
print(HCAdf1.shape)
# add extra rows for covid2 event so that every event with a date has a row
HCAdf2 = pd.concat([HCAdf1, Covid2], axis=0)
print(HCAdf2.shape)
# populate ages where not calculated
HCAdf2.loc[HCAdf2['event_date'].str.contains('2929'), 'event_date'] = ''
HCAdf2['alt_age'] = (pd.to_datetime(HCAdf2['event_date']) - pd.to_datetime(HCAdf2['dob'])).dt.days / 365.2425
HCAdf2.reset_index(inplace=True)
# assign missing ages to Covid Arms
HCAdf2.loc[HCAdf2.redcap_event.isin(['Covid', 'CR']), 'event_age'] = HCAdf2.alt_age
#remove empty rows (triggered by sending of survey to participant, where form status=0)
print(HCAdf2.shape)

HCAdf3=HCAdf2.loc[~(HCAdf2.sub_event.isin(['7.Covid1']) & (HCAdf2.event_date.astype('str')=='') & (HCAdf2.covid_complete=='0'))]
print(HCAdf3.shape)
HCAdf3=HCAdf3.loc[~(HCAdf3.sub_event.isin(['8.Covid2']) & (HCAdf3.event_date.astype('str')=='') & (HCAdf3.covid_2_complete=='0'))]
print(HCAdf3.shape)
HCAdf3=HCAdf3.loc[~(HCAdf3.sub_event.isin(['9.Covid Remote']) & (HCAdf3.event_date.astype('str')=='') & (HCAdf3.covid_remote_complete=='0'))]
print(HCAdf3.shape)
#remove empty rows for events that were triggered but have no registration
HCAdf3=HCAdf3.loc[~(HCAdf3.redcap_event.isin(['V1']) & (HCAdf3.event_register.astype('str')==''))]
print(HCAdf3.shape)
HCAdf3=HCAdf3.loc[~(HCAdf3.redcap_event.isin(['F1']) & (HCAdf3.event_register.astype('str')==''))]
print(HCAdf3.shape)
HCAdf3=HCAdf3.loc[~(HCAdf3.redcap_event.isin(['V2']) & (HCAdf3.event_register.astype('str')==''))]
print(HCAdf3.shape)
HCAdf3=HCAdf3.loc[~(HCAdf3.redcap_event.isin(['F2']) & (HCAdf3.event_register.astype('str')==''))]
print(HCAdf3.shape)
HCAdf3=HCAdf3.loc[~(HCAdf3.redcap_event.isin(['F3']) & (HCAdf3.event_register.astype('str')==''))]
print(HCAdf3.shape)
HCAdf3=HCAdf3.loc[~(HCAdf3.redcap_event.isin(['A']) & (HCAdf3.event_register.astype('str')==''))]
print(HCAdf3.shape)
HCAdf3=HCAdf3.sort_values(['site','sub_event','subject'])
#peak
HCAdf3[['redcap_event','sub_event','id','subject','event_date','event_age','event_register',
        'v1_date','age','visit','v3_date','age_v3','visit3','visit4','visit5','v2_date','age_v2','visit2',
        'v6_startdate','age_v6','visit6','covid_dt','visit7','covid_complete','bt_covid_dt','visit8','covid_2_complete','rt_covid_dt','visit9', 'covid_remote_complete','alt_age']].to_csv('HCAlong.csv',index=False)
HCAdf3=HCAdf3.loc[~((HCAdf3.event_date=="") | (HCAdf3.event_date.isnull()==True))]
#categorically drop the above housekeeping variables as well as any actigraphy variables so they don't slip in.
actcols=[a for a in HCAdf3.columns if 'agend' in a]
housecols=['event_register','v1_date','age','visit','v3_date','age_v3','visit3','visit4','visit5','v2_date','age_v2','visit2',
        'v6_startdate','age_v6','visit6','covid_dt','visit7','covid_complete','bt_covid_dt','visit8','covid_2_complete','rt_covid_dt','visit9', 'covid_remote_complete','alt_age',
           'register_subject_complete', 'register_visit1_complete', 'register_visit2_complete',
           'register_actigraphy_complete', 'register_follow_up_complete', 'register_follow_up2_complete',
           'register_follow_up3_complete', 'register_covid_complete', 'visit8_notes', 'register_covid_2_complete',
           'visit9_notes', 'register_covid_remote_complete', 'v1_rater', 'v2_rater', 'v3_rater', 'v6_enddate',
           'v6_rater', 'v6_data', 'v6_notes', 'register_visit_complete'  ]
#keep V1,V2,CR,and F events, if subject is in Freeze2.
print(HCAdf3.shape)
HCAdf3=HCAdf3.loc[HCAdf3.redcap_event.isin(['V1','V2','F1','F2','F3','CR'])].drop(columns=actcols+housecols)
print(HCAdf3.shape)
HCAdf3=HCAdf3.loc[HCAdf3.subject.isin(list(F2list.subject))]
HCAdf3['PIN']=HCAdf3.subject+'_'+HCAdf3.redcap_event
print(HCAdf3.shape)

#last one the V2 oops
v2oops=['HCA6686191_V2','HCA7296183_V2']
HCAdf3=HCAdf3.loc[~(HCAdf3.PIN.isin(v2oops))]
print(HCAdf3.shape)

#harmonize lists of variables:
ipaqrt=[a for a in HCAdf3.columns if 'rt_' in a and 'ipaq' in a]
psqirt=[a for a in HCAdf3.columns if 'rt_' in a and 'psqi' in a]
mocart=[a for a in HCAdf3.columns if 'rt_' in a and 'moca' in a]
oasrfp=[a for a in HCAdf3.columns if 'fp_' in a and 'oasr' in a]
asrfp=[a for a in HCAdf3.columns if 'fp_' in a and 'asr' in a and 'oasr' not in a]
strawfp=[a for a in HCAdf3.columns if 'fp_' in a and 'straw' in a]
mstrfp=[a for a in HCAdf3.columns if 'fp_' in a and 'mstr' in a]
iadlfp=[a for a in HCAdf3.columns if 'fp_' in a and 'iadl' in a]

def consolidate_prefixed(df, prefix, prefixed_vars, drop_prefixed=True):
    """
    For each prefixed column in prefixed_vars (e.g. 'rt_ipaq1'), copy values
    into its base column (e.g. 'ipaq1') when the base is missing/blank,
    then optionally drop the prefixed columns.
    """
    df_new = df.copy()

    for pref in prefixed_vars:
        if pref not in df_new.columns:
            continue
        base = pref[len(prefix):]  # strip prefix to get base name

        # ensure base exists
        if base not in df_new.columns:
            df_new[base] = pd.NA

        # Normalize blanks/whitespace -> pd.NA for string-like columns only
        for col in (pref, base):
            print(col)
            if pdt.is_string_dtype(df_new[col]) or df_new[col].dtype == object:
                # replace empty or whitespace-only strings with pd.NA
                df_new[col] = df_new[col].replace(r'^\s*$', pd.NA, regex=True)

        # mask: rows where base is missing but prefixed has a value
        mask = df_new[base].isna() & df_new[pref].notna()
        if mask.any():
            df_new.loc[mask, base] = df_new.loc[mask, pref]

        if drop_prefixed:
            df_new.drop(columns=[pref], inplace=True)

    return df_new


HCAdf4 = HCAdf3.copy()
HCAdf4.to_csv(outp+"test.csv",index=False)
HCAdf4=consolidate_prefixed(HCAdf4,"rt_",ipaqrt, drop_prefixed=True)
HCAdf4=consolidate_prefixed(HCAdf4,"rt_",psqirt, drop_prefixed=True)
HCAdf4=consolidate_prefixed(HCAdf4,"rt_",mocart, drop_prefixed=True)
HCAdf5=consolidate_prefixed(HCAdf4,"fp_",oasrfp, drop_prefixed=True)
HCAdf5=consolidate_prefixed(HCAdf4,"fp_",asrfp, drop_prefixed=True)
HCAdf5=consolidate_prefixed(HCAdf5,"fp_",strawfp, drop_prefixed=True)
HCAdf5=consolidate_prefixed(HCAdf5,"fp_",mstrfp, drop_prefixed=True)
HCAdf5=consolidate_prefixed(HCAdf5,"fp_",iadlfp, drop_prefixed=True)


#meds were indexed in CR.  Intractible
##medrt=[a for a in HCAdf3.columns if 'rt_' in a and 'med' in a]
##HCAdf3[['subject','redcap_event']+[a for a in HCAdf3.columns if 'med' in a]].to_csv('testmeds.csv',index=False)
#export meds for cleanup
HCAdf5meds=HCAdf5.loc[HCAdf5.redcap_event.isin(['V1','V2','V3','V4'])][['subject','redcap_event']+[f"med{i}" for i in range(1, 16)]].copy()

#meddict is a dictionary that was read in from a csv file of unique misspellings of medications and their clean names
cleanmeds=swapmeds(HCAdf5meds, meddict, [f"med{i}" for i in range(1, 16)])
#medcat is an actual json file
df=catmeds(cleanmeds,medcat, [f"med{i}" for i in range(1, 16)])

df.to_csv(os.path.join(outp,"Freeze2_HCA-Clean-Medications_"+ date.today().strftime("%Y-%m-%d") + '.csv'),index=False)
box.upload_file(os.path.join(outp,"Freeze2_HCA-Clean-Medications_"+ date.today().strftime("%Y-%m-%d") + '.csv'), PRCfolder)

HCAdf5.to_csv(os.path.join(outp,"HCA-RedCap_Processing_"+ date.today().strftime("%Y-%m-%d") + '.csv'),index=False)
box.upload_file(os.path.join(outp,"HCA-RedCap_Processing_"+ date.today().strftime("%Y-%m-%d") + '.csv'), PRCfolder)



##########################################
# ####### AABC
##########################################
#get full AABC redcap and prepare subject, event, and PIN - WITH ADDENDUM VARS FROM FREEZE 2 AND WITH ALL AF0 VARS ROLLED UP to first visit, and with the
#universal variables rolled up to all visits.

fullredAABC=getfullRedExport('aabcarms',secret)
REDCapAABC=fullredAABC.copy()
#drop all the ssaga variables so we're not carting them around
REDCapAABC = REDCapAABC.drop(columns=REDCapAABC.loc[:, "dp1":"dr_notes"].columns)
REDCapAABC['redcap_event'] = REDCapAABC.replace({'redcap_event_name':
                                               config['Redcap']['datasources']['aabcarms']['AABCeventmap']})['redcap_event_name']
AF0vars=['psuedo_guid','tics_score','croms_educ','croms_income','diagnosis_mci_ad','croms_cog','croms_occupation','croms_marital','croms_livingsit','croms_language','croms_insurance','croms_gender_identity','croms_gender_idother','croms_sexorientother','fips_state','fips_county','fips_censustract','fips_censusblock']
allvisitvars=['site','sex','subject_id']

#first roll the all-visit vars.
rolltoall=REDCapAABC.loc[REDCapAABC.redcap_event_name.str.contains('register')][['study_id']+allvisitvars]
rolltoall=rolltoall.rename(columns={'subject_id':'subject'})
REDCapAABC2=pd.merge(rolltoall,REDCapAABC.drop(columns=allvisitvars),on='study_id',how='left')

# now roll the af0 vars to first visit.
### first get the first event
firstevent=REDCapAABC2.loc[REDCapAABC2.redcap_event.str.contains("V")][['redcap_event','subject']].sort_values(['subject','redcap_event']).drop_duplicates('subject')
### now get the af0 vars together with the sexorient variable that needs special handling.
rolltofirstV=REDCapAABC2.loc[REDCapAABC2.redcap_event_name.str.contains('register')][['subject','redcap_event']+AF0vars]
sexorient=pd.read_csv(box.downloadFile('1993831566941'),low_memory=False,dtype={'croms_sexorient':str}).rename(columns={'subject_id':'subject'})
rolltofirstV=pd.merge(rolltofirstV,sexorient,on='subject',how='inner')
rollV=pd.merge(rolltofirstV.drop(columns='redcap_event'),firstevent,on='subject',how='inner')

REDCapAABC3=pd.merge(rollV,REDCapAABC2.drop(columns=AF0vars),on=['subject','redcap_event'],how='right').rename(columns={'study_id':'id'})
REDCapAABC3['PIN']=REDCapAABC3.subject+'_'+REDCapAABC3.redcap_event

#redcap needs to be filtered to the f2list but include the FU records that came in prior to those visits.  i.e.
#send REDCapAABC3 to Processing folder
REDCapAABC3=REDCapAABC3.loc[REDCapAABC3.subject.isin(list(F2list.subject))].copy()
REDCapAABC3['event_date'] = pd.to_datetime(REDCapAABC3['event_date'])
REDCapAABC3.loc[REDCapAABC3['event_date'].isnull()==True,'event_date'] = REDCapAABC3['v0_date']

REDCapAABC4 =REDCapAABC3.loc[(REDCapAABC3.event_date <= '2025-03-31')]
REDCapAABC4 =REDCapAABC4.loc[~(REDCapAABC4.subject=="HCA6539885")]
pcmp=['HCA7025859']
REDCapAABC4=REDCapAABC4.loc[~(REDCapAABC4.subject=='HCA7025859')]

afu=pd.read_csv(box.downloadFile('1993837828756'),low_memory=False).rename(columns={'study_id':'id'})
REDCapAABC5=pd.merge(REDCapAABC4,afu,on=['id','redcap_event_name'],how='left')

REDCapAABC5meds=REDCapAABC5.loc[REDCapAABC5.redcap_event.isin(['V1','V2','V3','V4'])][['subject','redcap_event']+[f"med{i}" for i in range(1, 16)]].copy()
#meddict is a dictionary that was read in from a csv file of unique misspellings of medications and their clean names
cleanmedsA=swapmeds(REDCapAABC5meds, meddict, [f"med{i}" for i in range(1, 16)])
cleanmedsA=cleanmedsA.replace(np.nan,"")

#medcat is an actual json file
df=catmeds(cleanmedsA,medcat, [f"med{i}" for i in range(1, 16)])
df.to_csv(os.path.join(outp,"Freeze2_AABC-Clean-Medications_"+ date.today().strftime("%Y-%m-%d") + '.csv'),index=False)
box.upload_file(os.path.join(outp,"Freeze2_AABC-Clean-Medications_"+ date.today().strftime("%Y-%m-%d") + '.csv'), PRCfolder)

REDCapAABC5.to_csv(os.path.join(outp,"AABC-RedCap_Processing_"+ date.today().strftime("%Y-%m-%d") + '.csv'),index=False)
box.upload_file(os.path.join(outp,"AABC-RedCap_Processing_"+ date.today().strftime("%Y-%m-%d") + '.csv'), PRCfolder)


#FIGURE OUT WHAT NEEDS TO BE ADDED TO CODEX
E=BalsaDictionary = pd.read_excel(box.downloadFile(config['balsadict']))
CNew=['Order','VarOrder','Variable','Form','Options','FSH','FSA','HCA','AABC','Prefix','Description','See','Access','Balsa_var','Balsa_label','CategoryOrder','Category','Balsa_Access','ExtendedOrder','ORorder','DisplayOrder','DisplayName', 'VIV']
E.columns=CNew
E.loc[E.FSA.isnull()==True,'FSA']=''
E.loc[E.FSH.isnull()==True,'FSH']=''

compare=E[['FSA','FSH','Variable']]

HCADICT=pd.read_csv("HCPAV13Destination_DataDictionary_2025-09-22_Freeze2check.csv",low_memory=False)[['Variable / Field Name', 'Form Name', 'Section Header', 'Field Type',
       'Field Label', 'Choices, Calculations, OR Slider Labels', 'Field Note']]
HCADICT=HCADICT.rename(columns={'Variable / Field Name':'Variable'})
HCADICT=HCADICT.loc[~(HCADICT['Field Type']=='descriptive')]
HCADICT=HCADICT.loc[~(HCADICT['Form Name'].isin(['covid','covid_2','covid_remote_summary']))]
oth_unusedH=ipaqrt+psqirt+mocart+oasrfp+asrfp+strawfp+mstrfp+iadlfp+[b for b in list(HCADICT.Variable) if "bt_ipaq" in b or "bt_psqi" in b]+[a for a in list(HCADICT.Variable) if "bt_" in a]
oth=['rt_med_num','rt_total_med','rt_days_covidrt_covid2','rt_days_covidrt_v2','rt_days_covidrt_v1','rt_med_latestv','rt_med_num_latestv','rt_med_yn']
missH=[a for a in list(HCADICT.Variable) if a not in list(compare.Variable) and 'batlq_' not in a and 'bt_med' not in a and a not in oth_unusedH and a not in oth]
missH2=[a for a in missH if 'med1' not in a and 'med2' not in a and 'med3' not in a and 'med4' not in a and 'med5' not in a and 'med6' not in a and 'med7' not in a and 'med8' not in a and 'med9' not in a]
missH3=[a for a in missH2 if 'sad1' not in a and 'sad2' not in a and 'sad3' not in a and 'sad4' not in a and 'sad5' not in a and 'sad6' not in a and 'sad7' not in a]

AABCDICT=pd.read_csv("AABCARMS_DataDictionary_2025-09-18_Freeze2check.csv",low_memory=False)[['Variable / Field Name', 'Form Name', 'Section Header', 'Field Type',
       'Field Label', 'Choices, Calculations, OR Slider Labels', 'Field Note']]
AABCDICT=AABCDICT.rename(columns={'Variable / Field Name':'Variable'})
AABCDICT=AABCDICT.loc[~(AABCDICT['Field Type']=='descriptive')]

oth_unused=['croms_censusblock','bad_instructions','sad2_rate','sad3_rate','sad4_rate','sad5_rate','sad6_rate','sad7_rate','sad8_rate']
missA=[a for a in list(AABCDICT.Variable) if a not in list(compare.Variable) and "batlq" not in a and a not in oth_unused]


