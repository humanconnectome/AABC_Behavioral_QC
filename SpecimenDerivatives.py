import pandas as pd
from ccf.box import LifespanBox
import re
import collections
from functions import *
from config import *
from datetime import date
import requests
import numpy as np


# TO DO
#Plate threshold= flag
#80% failed = failed panel

###############
DNR = ["HCA7787304_V1", "HCA6276071_V1", "HCA6229365_V1", "HCA9191078_V1", "HCA6863086_V1"]
#These guys accidentally recruited as V2
v2oops=['HCA6686191_V2','HCA7296183_V2']
outp="/Users/petralenzini/work/Behavioral/AABC/AABC_Behavioral_QC/AABC_Behavioral_QC/tmp/"

## get configuration files
config = LoadSettings()
secret=pd.read_csv(config['config_files']['secrets'])
intradb=pd.read_csv(config['config_files']['PCP'])
box = LifespanBox(cache="./tmp")
Asnaps=config['aabc_pre']
freezefolder=config['unionfreeze1']

freeze=pd.read_csv(outp+"Freeze1_n439_12Feb2024_MR_ID.csv")
freeze.columns=['PIN']
freezelist=list(freeze.PIN)
pathp=box.downloadFile(config['hcainventory'])
ids=pd.read_csv(pathp)[['PIN','redcap_event','IntraDB']]
HCAlist=list(ids.loc[(~(ids.IntraDB=='CCF_PCMP_ITK')) & (ids.redcap_event.isin(['V1','V2'])) & (~(ids.PIN.isin(v2oops)))]['PIN'])

#GWAS
gwasfamfile=outp+"HCA_imputed_geno0.02_final.fam"
gwasHCA=list(pd.read_csv(gwasfamfile,sep='\t',header=None)[0].unique()) #949

#Metabolites
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
Mvalues.to_csv(outp+'MetabolitesTable.csv',index=False)

#AD biomarkers
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

AD['AD_Biomarkers']=1
AD.to_csv(outp+'ADBiomarkersTable.csv',index=False)

#PRS
PRS=pd.read_excel(box.downloadFile(config['PRS_HCA']),sheet_name='PRS')
PRS=PRS.rename(columns={'Unique_Pheno_ID':'subject'})
## get the HCA inventory for ID checking with AABC

#APOE
APOE_new=pd.read_excel(box.downloadFile(config['NewAPOE']),sheet_name='Sheet2').rename(columns={'Unique_Pheno_ID':'subject'})
APOE_old=pd.read_csv(box.downloadFile(config['OldAPOE']))
APOE_old['APOE_Genotype']=APOE_old.APOE_Genotype.replace(np.nan,-99999).astype(int).astype(str).replace('-99999','')

#concatenate them to test for discrepancies
APOE_Union=pd.concat([APOE_new[['subject','Genotype']],APOE_old[['subject','APOE_Genotype']].rename(columns={'APOE_Genotype':'Genotype'})],axis=0)
print(APOE_Union.shape)
APOE_Union.Genotype=APOE_Union.Genotype.astype('str').replace(' ','')
APOE_Union=APOE_Union.drop_duplicates()
print(APOE_Union.shape)
APOE_Union.to_csv(outp+'testAPOE.csv',index=False)
print(APOE_Union.shape)
print(len(APOE_Union.subject.unique()))

#nothing in the freeze so no special dataset needed.

SpecimenIDPs1=pd.merge(Mvalues,AD,on=['subject','redcap_event','PIN'],how='outer')
SpecimenIDPs1.shape
Union=SpecimenIDPs1.loc[(SpecimenIDPs1.PIN.isin(freezelist+HCAlist))][['PIN']+[i for i in SpecimenIDPs1.columns if i != 'PIN' and i !='Metabolites']+['Metabolites']]

SpecimenIDPs1.to_csv(outp+'AABC-HCA_Metabolites-AD-Biomarkers_2024-03-21.csv',index=False)
Union.to_csv(outp+'Freeze1_AABC-HCA_Metabolites-AD-Biomarkers_2024-03-21.csv',index=False)
box.upload_file(outp+'AABC-HCA_Metabolites-AD-Biomarkers_2024-03-21.csv', Asnaps)
box.upload_file(outp+'Freeze1_AABC-HCA_Metabolites-AD-Biomarkers_2024-03-21.csv', freezefolder)

union2=list(set([i.replace("_V1",'') for i in (HCAlist)]))
union3=list(set([i.replace("_V2",'') for i in union2]))
unionlist=list(set([i.replace("_V3",'') for i in union3]))

#genotype based
SpecimenIDPs2=pd.merge(PRS,APOE_Union,on=['subject'],how='outer').rename(columns={'Genotype':'APOE_Genotype'})
Union2=SpecimenIDPs2.loc[(SpecimenIDPs2.subject.isin(unionlist))]
SpecimenIDPs2.to_csv(outp+'AABC-HCA_APOE-PRS_2024-03-21.csv',index=False)
Union2.to_csv(outp+'Freeze1_AABC-HCA_APOE-PRS_2024-03-21.csv',index=False)
box.upload_file(outp+'AABC-HCA_Apoe-PRS_2024-03-21.csv', Asnaps)
box.upload_file(outp+'Freeze1_AABC-HCA_Apoe-PRS_2024-03-21.csv', freezefolder)
