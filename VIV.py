
from ccf.box import LifespanBox
import pandas as pd
import matplotlib.pyplot as plt
import os
import shutil
from datetime import date
from config import *
import numpy as np

config = LoadSettings()
secret=pd.read_csv(config['config_files']['secrets'])
intradb=pd.read_csv(config['config_files']['PCP'])
box = LifespanBox(cache="./tmp")
outp="/Users/petralenzini/work/Behavioral/AABC/AABC_Behavioral_QC/AABC_Behavioral_QC/tmp/"

##########################################################################
# Output is a table, slice dictionary, plots, distributions, and receipt

statsdir='/Users/petralenzini/work/Behavioral/AABC/AABC_Behavioral_QC/AABC_Behavioral_QC/tmp/'
statsfile='AABC_HCA_recruitmentstats.pdf'

# Now complete the following
datarequestor='VIV_Freeze1'  # name of folder in which you want to generate the slice.
study="HCA-AABC" #or HCA

#Do you want caveman distributions of plots?
wantplots = True  # or False
DownloadF = True

#########################################################################################################
################################### Done with user specs #####################

E = pd.read_csv(box.downloadFile(config['encyclopedia']), low_memory=False, encoding='ISO-8859-1')
C = pd.read_csv(box.downloadFile(config['completeness']), low_memory=False, encoding='ISO-8859-1')

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
    freezemain=pd.DataFrame(box.list_of_files([str(config['unionfreeze1'])])).transpose()
    #download
    for i in list(freezemain.fileid):
        if DownloadF:
            print("downloading",i,"...")
            box.downloadFile(i)
        else:
            print("already downloaded",i,"...")
    #also grab MRS data
    box.downloadFile(1479152634577)
except:
    print("Something went wrong")

# Merge non-toolbox visit specific stuff together
# merge in subject-specific stuff
# Stack, transpose, and merge TOOLBOX data.
# add calculated variables like BMI.

# subset to VIV or other variables of interest.

# THen stack studies together, incorporating exceptions for APOE, FAMILIES, and REgistration variables
widefile=C[['PIN','subject','redcap_event']].copy()
listfiles=os.listdir(os.path.join(os.getcwd(),datarequestor+'/downloadedfiles/'))
for i in listfiles:
    print("file:",i)
    if 'Actigraphy' in i or 'Totals' in i or 'REDCap' in i or 'STRAW' in i or 'PennCNP' in i or 'SSAGA' in i or 'Metabolites' in i:
        print("processing...")
        D=pd.read_csv(os.path.join(os.getcwd(),datarequestor+'/downloadedfiles/',i),low_memory=False)
        D['PIN']=D['subject']+D['redcap_event']
        check=[d for d in D.columns if d in widefile.columns and d not in ['subject','redcap_event','PIN']]
        if len(check)>0:
            print("NON-unique variables:",check)
        widefile=widefile.merge(D,on=['subject','redcap_event','PIN'])
    elif 'MRS' in i:
        print("processing...MRS")
        D=pd.read_excel(os.path.join(os.getcwd(),datarequestor+'/downloadedfiles/',i),sheet_name='conc_tCr',header=0)
        D['PIN'] = D['subject'] + D['redcap_event']
        check = [d for d in D.columns if d in widefile.columns and d not in ['subject', 'redcap_event', 'PIN']]
        if len(check) > 0:
            print("NON-unique variables:", check)
        widefile = widefile.merge(D, on=['subject', 'redcap_event', 'PIN'])
    elif 'Apoe' in i:
        print("processing...Apoe")
        D=pd.read_csv(os.path.join(os.getcwd(),datarequestor+'/downloadedfiles/',i),low_memory=False)
        if len(check)>0:
            print("NON-unique variables:",check)
        widefile=widefile.merge(D,on=['subject'])

#now for stuff that needs to be stacked
qfiles=[q for q in listfiles if 'Q-Interactive' in q]
Q=pd.concat([pd.read_csv(os.path.join(os.getcwd(),datarequestor+'/downloadedfiles/',qfiles[0])), pd.read_csv(os.path.join(os.getcwd(),datarequestor+'/downloadedfiles/',qfiles[1]))],axis=0)
widefile=widefile.merge(Q,on=['subject','redcap_event','PIN'])

#Toolbox needs to be transposed.
Tfiles=[t for t in listfiles if 'NIH' in t and "Scores" in t]
t1=pd.read_csv(os.path.join(os.getcwd(),datarequestor+'/downloadedfiles/',Tfiles[0]),low_memory=False)
t2=pd.read_csv(os.path.join(os.getcwd(),datarequestor+'/downloadedfiles/',Tfiles[1]),low_memory=False)

T=pd.concat([t1,t2]),axis=0)

if 'NIH' in i and 'Scores' in i:
    print(i)
    bulk = pd.read_csv(os.path.join(os.getcwd(), datarequestor + '/downloadedfiles/' + i), low_memory=False)
    l = []
    tlbxlist = [j for j in InstRequest if
                ('NIH' in j) or ('Cognition' in j and 'Composite' in j) or ('Summary (18+)' in j)]
    for t in tlbxlist:
        l2 = [b for b in bulk.Inst.unique() if t in b]
        l = l + l2
    bulk = bulk.loc[bulk.Inst.isin(l)].copy()
    if not bulk.empty:
        bulk['subject'] = bulk.PIN.str.split('_', expand=True)[0]
        bulk['redcap_event'] = bulk.PIN.str.split('_', expand=True)[1]
        for k in tlbxlist:
            print(k)
            tlbxvars = Evars.loc[Evars['Form / Instrument'] == k][
                ['Variable / Field Name', 'NIH Toolbox Prefix in Slice', 'Unavailable']]
            tlbxvars['newname'] = tlbxvars['NIH Toolbox Prefix in Slice'] + '_' + tlbxvars['Variable / Field Name']
            tlbxvars['newname'] = tlbxvars.newname.str.replace(' ', '_')
            mapvars = dict(
                zip(list(tlbxvars['Variable / Field Name']), list(tlbxvars.newname)))  # ,tlbxvars['newname'])
            temp = bulk.loc[bulk.Inst.str.contains(k)][
                ['subject', 'redcap_event'] + list(tlbxvars['Variable / Field Name'])]
            temp = temp.rename(columns=mapvars)
            for j in ['site', 'study', 'PIN', 'redcap_event_name', 'site', 'study_id', 'id', 'gender']:
                try:
                    temp = temp.drop(columns=[j]).copy()
                except:
                    pass
            if 'AABC' in i:
                widefileAABC = pd.merge(widefileAABC, temp, on=['subject', 'redcap_event'], how='outer')
            if 'HCA' in i:
                widefileHCA = pd.merge(widefileHCA, temp, on=['subject', 'redcap_event'], how='outer')

    else:
        pass: (don't want to merge encyclopedia or completeness')

        df=pd.read_csv(os.path.join(os.getcwd(),datarequestor+'/downloadedfiles/'+i),low_memory=False)
        colkeep=[i for i in list(df.columns) if i in Evars+['PIN']]
        print(colkeep)
        if len(colkeep)>3:
            dfsub=df[colkeep].copy()
            widefile=pd.merge(widefile,dfsub,on=['PIN','subject','redcap_event'])



                if 'AABC' in i:
                    widefileAABC=pd.merge(widefileAABC,temp, on=['subject','redcap_event'],how='outer')
                if 'HCA' in i:
                    widefileHCA = pd.merge(widefileHCA, temp, on=['subject', 'redcap_event'], how='outer')
wide.redcap_event.value_counts()


#clean up age variable
wide.event_age=wide.event_age.round(1)
#calculate BMI from height and weight for HCA

#don't subset by event...drop missing rows
wide['countmiss']=wide.isna().sum(axis=1)
wide.countmiss.value_counts()

#subset to VIV or specific instruments

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

sliceout.loc[sliceout.Union_Freeze1_Nov2023==1 & sliceout.IntraDB.isin(['CCF_HCA_STG','AABC_STG'])].redcap_event.value_counts()
len(sliceout.loc[sliceout.Union_Freeze1_Nov2023==1 & sliceout.IntraDB.isin(['CCF_HCA_STG','AABC_STG'])].subject.unique())

firstcols=['study','Cohort','subject','HCAAABC_event','PIN','event_age','race','ethnic_group','M/F','site','IntraDB','AABC_Freeze1_Nov2023','Union_Freeze1_Nov2023']
lastcols=[col for col in sliceout.columns if col not in firstcols and col not in droplist]
sliceout=sliceout[firstcols+lastcols].drop(columns=['PIN','redcap_event_name','HCA_Freeze1_Nov2023']).copy()
sliceout.to_csv(os.path.join(os.getcwd(),datarequestor+"/"+study+"_Slice_"+ date.today().strftime("%Y-%m-%d") + '.csv'),index=False)

print("HCA-AABC Totals: \n",sliceout.loc[(sliceout.redcap_event.isin(['V1','V2','V3','V4']))].redcap_event.value_counts())
print("AABC Freeze Totals: \n",sliceout.loc[(sliceout.AABC_Freeze1_Nov2023==1)  & (sliceout.redcap_event.isin(['V1','V2','V3','V4']))].redcap_event.value_counts())
print("AABC Freeze with Metabolites: \n",sliceout.loc[(sliceout.AABC_Freeze1_Nov2023==1) & (sliceout.Metabolites==1) & (sliceout.redcap_event.isin(['V1','V2','V3','V4']))].redcap_event.value_counts())
print("AABC Freeze with AD Biomarkers: \n",sliceout.loc[(sliceout.AABC_Freeze1_Nov2023==1) & (sliceout.AD_Biomarkers==1)].redcap_event.value_counts())

print("HCA-AABC Union Freeze Totals: \n",sliceout.loc[(sliceout.Union_Freeze1_Nov2023==1) & (sliceout.redcap_event.isin(['V1','V2','V3','V4']))].redcap_event.value_counts())
print("HCA-AABC Union Freeze with Metabolites: \n",sliceout.loc[(sliceout.Union_Freeze1_Nov2023==1) & (sliceout.Metabolites==1) ].redcap_event.value_counts())
print("HCA-AABC Union Freeze with AD Biomarkers: \n",sliceout.loc[(sliceout.Union_Freeze1_Nov2023==1) & (sliceout.AD_Biomarkers==1) ].redcap_event.value_counts())

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


slicevars=[i for i in list(sliceout.columns)]# if i not in ['redcap_event','subject','study']]

headerE=E.loc[(E['Variable / Field Name'].isin(['subject','redcap_event'])) & (E['Form / Instrument']=='SUBJECT INVENTORY AND BASIC INFORMATION')]
Evars=Evars.copy()
Evars['newname']=Evars['Variable / Field Name']
Evars.loc[Evars['NIH Toolbox Prefix in Slice'].isnull()==False,'newname']= Evars['NIH Toolbox Prefix in Slice']+'_'+Evars['Variable / Field Name']
D=pd.concat([headerE,Evars.loc[Evars['newname'].isin(slicevars)]])
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
print("Links:",file=file_object)
print("", file=file_object)
print("AABC Pre-Release Folder in Box: https://wustl.box.com/s/9gnrbyq7fybw0wtd82zfoagki2d5uky1", file=file_object)
print("HCA Pre-Release Folder in Box: https://wustl.box.com/s/9gnrbyq7fybw0wtd82zfoagki2d5uky1", file=file_object)
print("Encyclopedia: https://wustl.box.com/s/kr7lfj1finvcblr0ye0ls39gglo9xqbx", file=file_object)
print("", file=file_object)
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
