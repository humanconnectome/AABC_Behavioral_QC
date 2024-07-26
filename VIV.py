
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
datarequestor='AVIV4'  # name of folder in which you want to generate the slice.
study="HCA-AABC" #or HCA
currentcogfolder=262328612623 # Nichols_2024-03-27  Nichols_CogFactorRegen_6May2024

#Do you want caveman distributions of plots?
wantplots = False
DownloadF = True

#can do either the VIV or a specific instrument list - Instrument lists need to be case sensitive strings from Encyclopedia
VIV=True
InstRequested=[]
#InstRequested=['Completeness Inventory','NIH Toolbox Pattern Comparison Processing Speed Test','NIH Toolbox Picture Sequence Memory Test','NIH Toolbox Picture Vocabulary Test','NIH Toolbox Visual Acuity Test','NIH Toolbox Words-In-Noise Test','NIH Toolbox Dimensional Change Card Sort Test','NIH Toolbox Flanker Inhibitory Control and Attention Test','NIH Toolbox List Sorting Working Memory Test','NIH Toolbox Oral Reading Recognition Test','Cognition Crystallized Composite','Cognition Fluid Composite','Cognition Total Composite Score','Montreal Cognitive Assessment (MOCA)','Trail Making Scores','Q-Interactive Ravlt']
if len(InstRequested)>0:
    VIV=False
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
    if DownloadF:
        box.downloadFile(1479152634577)
except:
    print("Something went wrong")

# Merge non-toolbox visit specific stuff together
# SSAGA needs to be merged with HCA and then stacked with AABC
# merge in subject-specific stuff
# Stack, transpose, and merge TOOLBOX data.
# add calculated variables like BMI.
# keep VIV variables; be careful about checkbox vars
# lastly subset to VIV or other variables of interest.

# THen stack studies together, incorporating exceptions for APOE, FAMILIES, and REgistration variables
widefile=C[['PIN','subject','redcap_event','event_age']].copy()
listfiles=os.listdir(os.path.join(os.getcwd(),datarequestor+'/downloadedfiles/'))
print(widefile.shape)
for i in listfiles:
    print("file:",i)
    if 'Actigraphy' in i or 'Totals' in i or 'STRAW' in i or 'PennCNP' in i or 'Metabolites' in i:
        print("processing...")
        D=pd.read_csv(os.path.join(os.getcwd(),datarequestor+'/downloadedfiles/',i),low_memory=False)
        D['PIN']=D['subject']+"_"+D['redcap_event']
        check=[d for d in D.columns if d in widefile.columns and d not in ['subject','redcap_event','PIN']]
        if len(check)>0:
            print("NON-unique variables:",check)
        D=D.drop_duplicates(subset=['subject','redcap_event'])
        widefile=widefile.merge(D,on=['subject','redcap_event','PIN'],how='left')
        print(i,widefile.shape)
    elif 'MRS' in i:
        print("processing...MRS")
        D=pd.read_excel(os.path.join(os.getcwd(),datarequestor+'/downloadedfiles/',i),sheet_name='conc_tCr')
        D['PIN'] = D['subject'] +"_"+ D['redcap_event']
        print(i,D.shape)
        check = [d for d in D.columns if d in widefile.columns and d not in ['subject', 'redcap_event', 'PIN']]
        if len(check) > 0:
            print("NON-unique variables:", check)
        widefile = widefile.merge(D, on=['subject', 'redcap_event','PIN'],how='left')
        print(i,widefile.shape)
    elif 'Apoe' in i:
        print("processing...Apoe")
        D=pd.read_csv(os.path.join(os.getcwd(),datarequestor+'/downloadedfiles/',i),low_memory=False)
        if len(check)>0:
            print("NON-unique variables:",check)
        widefile=widefile.merge(D, on=['subject'],how='left')
        print(i,widefile.shape)

#now for stuff that needs to be stacked
#redcap
redHfile=[r for r in listfiles if 'RedCap' in r and 'HCA' in r]
redAfile=[r for r in listfiles if 'RedCap' in r and 'AABC' in r]
rh=pd.read_csv(os.path.join(os.getcwd(),datarequestor+'/downloadedfiles/',redHfile[0]),low_memory=False)
ra=pd.read_csv(os.path.join(os.getcwd(),datarequestor+'/downloadedfiles/',redAfile[0]),low_memory=False)

ss=[s for s in listfiles if 'SSAGA' in s]
sss=pd.read_csv(os.path.join(os.getcwd(),datarequestor+'/downloadedfiles/',ss[0]),low_memory=False)
rhh=pd.merge(rh,sss,on=['PIN','redcap_event','subject'],how='left')

R=pd.concat([ra,rhh],axis=0)
#special merge
SpecialAF0=R.loc[R.redcap_event=='AF0'][['subject','croms_income','tics_score','cbf1_1','cbf2_1']].copy()
SpecialAF0.columns=['subject','croms_income_AF0','tics_score_AF0','cbf1_1_AF0','cbf2_1_AF0']
R=pd.merge(R,SpecialAF0,on='subject',how='left')

R['PIN'] = R['subject'] + "_" + R['redcap_event']
widefile=pd.merge(widefile,R,on=['subject', 'redcap_event','PIN'],how='left')
print(i,widefile.shape)

#RAVLT
qfiles=[q for q in listfiles if 'Q-Interactive' in q]
Q=pd.concat([pd.read_csv(os.path.join(os.getcwd(),datarequestor+'/downloadedfiles/',qfiles[0])), pd.read_csv(os.path.join(os.getcwd(),datarequestor+'/downloadedfiles/',qfiles[1]))],axis=0)
widefile=widefile.merge(Q,on=['subject','redcap_event','PIN'],how='left')
print(i,widefile.shape)
#add RAVLT calculations
#Immediate Recall
widefile.ravlt_pea_ravlt_sd_trial_i_tc=widefile.ravlt_pea_ravlt_sd_trial_i_tc.str.upper().str.replace("*NULL",'',regex=False)
widefile.ravlt_pea_ravlt_sd_trial_ii_tc=widefile.ravlt_pea_ravlt_sd_trial_ii_tc.str.upper().str.replace("*NULL",'',regex=False)
widefile.ravlt_pea_ravlt_sd_trial_iii_tc=widefile.ravlt_pea_ravlt_sd_trial_iii_tc.str.upper().str.replace("*NULL",'',regex=False)
widefile.ravlt_pea_ravlt_sd_trial_iv_tc=widefile.ravlt_pea_ravlt_sd_trial_iv_tc.str.upper().str.replace("*NULL",'',regex=False)
widefile.ravlt_pea_ravlt_sd_trial_v_tc=widefile.ravlt_pea_ravlt_sd_trial_v_tc.str.upper().str.replace("*NULL",'',regex=False)
widefile.ravlt_pea_ravlt_sd_trial_vi_tc=widefile.ravlt_pea_ravlt_sd_trial_vi_tc.str.upper().str.replace("*NULL",'',regex=False)
widefile.ravlt_pea_ravlt_sd_trial_i_tc=widefile.ravlt_pea_ravlt_sd_trial_i_tc.str.upper().str.replace("NULL*",'',regex=False)
widefile.ravlt_pea_ravlt_sd_trial_ii_tc=widefile.ravlt_pea_ravlt_sd_trial_ii_tc.str.upper().str.replace("NULL*",'',regex=False)
widefile.ravlt_pea_ravlt_sd_trial_iii_tc=widefile.ravlt_pea_ravlt_sd_trial_iii_tc.str.upper().str.replace("NULL*",'',regex=False)
widefile.ravlt_pea_ravlt_sd_trial_iv_tc=widefile.ravlt_pea_ravlt_sd_trial_iv_tc.str.upper().str.replace("NULL*",'',regex=False)
widefile.ravlt_pea_ravlt_sd_trial_v_tc=widefile.ravlt_pea_ravlt_sd_trial_v_tc.str.upper().str.replace("NULL*",'',regex=False)
widefile.ravlt_pea_ravlt_sd_trial_vi_tc=widefile.ravlt_pea_ravlt_sd_trial_vi_tc.str.upper().str.replace("NULL*",'',regex=False)
widefile.ravlt_pea_ravlt_sd_trial_i_tc = pd.to_numeric(widefile.ravlt_pea_ravlt_sd_trial_i_tc,errors='coerce')
widefile.ravlt_pea_ravlt_sd_trial_ii_tc = pd.to_numeric(widefile.ravlt_pea_ravlt_sd_trial_ii_tc,errors='coerce')
widefile.ravlt_pea_ravlt_sd_trial_iii_tc = pd.to_numeric(widefile.ravlt_pea_ravlt_sd_trial_iii_tc,errors='coerce')
widefile.ravlt_pea_ravlt_sd_trial_iv_tc = pd.to_numeric(widefile.ravlt_pea_ravlt_sd_trial_iv_tc,errors='coerce')
widefile.ravlt_pea_ravlt_sd_trial_v_tc = pd.to_numeric(widefile.ravlt_pea_ravlt_sd_trial_v_tc,errors='coerce')
widefile.ravlt_pea_ravlt_sd_trial_vi_tc = pd.to_numeric(widefile.ravlt_pea_ravlt_sd_trial_vi_tc,errors='coerce')



widefile['ravlt_immediate_recall']=widefile.ravlt_pea_ravlt_sd_trial_i_tc.astype(float)+widefile.ravlt_pea_ravlt_sd_trial_ii_tc.astype(float)+widefile.ravlt_pea_ravlt_sd_trial_iii_tc.astype(float)+widefile.ravlt_pea_ravlt_sd_trial_iv_tc.astype(float)+widefile.ravlt_pea_ravlt_sd_trial_v_tc.astype(float)
widefile['ravlt_learning_score']=widefile.ravlt_pea_ravlt_sd_trial_v_tc.astype(float)-widefile.ravlt_pea_ravlt_sd_trial_i_tc.astype(float)

#Toolbox needs to be transposed.
Tfiles=[t for t in listfiles if 'NIH' in t and "Scores" in t]
t1=pd.read_csv(os.path.join(os.getcwd(),datarequestor+'/downloadedfiles/',Tfiles[0]),low_memory=False)
t2=pd.read_csv(os.path.join(os.getcwd(),datarequestor+'/downloadedfiles/',Tfiles[1]),low_memory=False)
T=pd.concat([t1,t2],axis=0)

bulk = T.copy()
l = []
wide=C[['subject', 'redcap_event','PIN']]
tlbxlist = [j for j in list(E['Form / Instrument'].unique()) if ('Covid' not in j) and ('Descriptions' not in j) and (('NIH' in j) or ('Cognition' in j and 'Composite' in j) or ('Summary (18+)' in j))]
for t in tlbxlist:
    l2 = [b for b in bulk.Inst.unique() if t in b]
    l = l + l2
bulk = bulk.loc[bulk.Inst.isin(l)].copy()
if not bulk.empty:
    bulk['subject'] = bulk.PIN.str.split('_', expand=True)[0]
    bulk['redcap_event'] = bulk.PIN.str.split('_', expand=True)[1]
    for k in tlbxlist:
        print(k)
        tlbxvars = E.loc[(E['Form / Instrument']==k) & (~(E.Unavailable =='U'))][['Variable / Field Name', 'NIH Toolbox Prefix in Slice', 'Unavailable']]
        tlbxvars['newname'] = tlbxvars['NIH Toolbox Prefix in Slice'] + '_' + tlbxvars['Variable / Field Name']
        tlbxvars['newname'] = tlbxvars.newname.str.replace(' ', '_')
        mapvars = dict(
            zip(list(tlbxvars['Variable / Field Name']), list(tlbxvars.newname)))  # ,tlbxvars['newname'])
        if 'Negative Affect Summary' in k:
            k='Negative Affect Summary'
        if 'Psychological Well Being' in k:
            k = 'Psychological Well Being'
        if 'Social Satisfaction Summary' in k:
            k = 'Social Satisfaction Summary'
        temp = bulk.loc[bulk.Inst.str.contains(k)][['subject', 'redcap_event'] + list(tlbxvars['Variable / Field Name'])]
        temp = temp.rename(columns=mapvars)
        for j in ['site', 'study', 'PIN', 'redcap_event_name', 'site', 'study_id', 'id', 'gender']:
            try:
                temp = temp.drop(columns=[j]).copy()
            except:
                pass
        temp=temp.drop_duplicates(subset=['subject','redcap_event'])
        wide = pd.merge(wide, temp, on=['subject', 'redcap_event'], how='outer')


widefile=pd.merge(widefile,wide,on=['subject','redcap_event','PIN'],how='left')
print(i,widefile.shape)
#DONT clean up age variable - not good for cog factor consistency.  Better to make note in Known issues and FAQ
#widefile.event_age=widefile.event_age.round(1)

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
#calculate BMI from height and weight for HCA
widefile.loc[widefile.height_ft.isnull(),'height_ft']=widefile.height.str.split("'",expand=True)[0]
widefile.loc[widefile.height_in.isnull(),'height_in']=widefile.height.str.split("'",expand=True)[1]
widefile['total_inches']=12 * widefile.height_ft.astype(float) + widefile.height_in.astype(float)
widefile['bmi']=(widefile['weight']/(widefile['total_inches']*widefile['total_inches'])*703).round(2)

#don't subset by event...drop missing rows
widefile['countmiss']=widefile.isna().sum(axis=1)
widefile.countmiss.value_counts()
widefile=widefile.loc[~(widefile.event_age <36)]
print(i,widefile.shape)

#subset to VIV or specific instruments
VarlistRoot=E.loc[(~(E.Unavailable =='U')) & (~(E['Form / Instrument'].str.upper().str.contains('PANAS')))].copy()
VarlistRoot['newname']=VarlistRoot['Variable / Field Name']
VarlistRoot.loc[VarlistRoot['NIH Toolbox Prefix in Slice'].isnull()==False,'newname']=VarlistRoot['NIH Toolbox Prefix in Slice'] + '_' + VarlistRoot['Variable / Field Name']
VarlistRoot.loc[VarlistRoot['NIH Toolbox Prefix in Slice'].isnull()==False,'newname'] = VarlistRoot.newname.str.replace(' ', '_')


#condition on variable inclusion
if VIV:
    Varlist=list(VarlistRoot.loc[VarlistRoot.VIV=='V']['newname'])
    D = VarlistRoot.loc[VarlistRoot.VIV=='V'].copy()
if VIV==False:
    D = VarlistRoot.loc[VarlistRoot['Form / Instrument'].isin(InstRequested)].copy()
    Varlist=list(VarlistRoot.loc[VarlistRoot['Form / Instrument'].isin(InstRequested)]['newname'])

widecols=[i for i in list(widefile.columns) if i in Varlist]
jk=[j for j in Varlist if j in list(widefile.columns)]

sliceout=widefile[widecols]
extras=['PIN','Cohort',
     'Site',
     'race',
     'ethnic_group',
     'pedid',
     'M/F']
sliceout=pd.merge(C[extras],sliceout,on='PIN',how='left')

#clean up missing vars

if VIV:
    sliceout.to_csv(os.path.join(os.getcwd(),datarequestor+"/Union-Freeze_AABC-HCA_VIV_"+ date.today().strftime("%Y-%m-%d") + '.csv'),index=False)
    #box.upload_file(os.path.join(os.getcwd(),datarequestor+"/Union-Freeze_AABC-HCA_VIV_"+ date.today().strftime("%Y-%m-%d") + '.csv'), '250512318481')



#probably a bug here
#fix so that its grabbing all the checkbox variables!!!!!!
#line 213 of sendSnapshot

if VIV==False:
    sliceout.to_csv(os.path.join(os.getcwd(),datarequestor+"/Union-Freeze_AABC-HCA_Slice_"+ date.today().strftime("%Y-%m-%d") + '.csv'),index=False)
    D=D.loc[D['newname'].isin(list(sliceout.columns)+extras)]

print("HCA-AABC Totals: \n",sliceout.loc[(sliceout.redcap_event.isin(['V1','V2','V3','V4']))].redcap_event.value_counts())
D.drop(columns=['newname']).to_csv(os.path.join(os.getcwd(),datarequestor+"/"+"Freeze1_AABC-HCA_Slice_Dictionary_"+ date.today().strftime("%Y-%m-%d") + '.csv'),index=False)

###
#get the precise filenames downlaoded for the receipt
y = [i for i in listfiles if i != '.DS_Store']

#plots:
skip_plots=['subject','redcap_event','PIN','Actigraphy_Cobra','HCA_Freeze1_Nov2023','COMMENTS','Notes']
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
            plt.xlabel(i)
            plt.ylabel('count')
            varn=D.loc[D['newname']==i].reset_index()
            print(i)
            tital=varn['Variable Description'][0]
            print(tital)
            if 'tlbx' in str(varn['NIH Toolbox Prefix in Slice'][0]):
                tital=varn['Form / Instrument'][0]+" : "+i
            print(tital)
            plt.title(tital,wrap=True)
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
#print("Most Recent Recruitment Report:",statsfile,file=file_object)
#print("Slice:","Freeze1_AABC-HCA_Slice_"+ date.today().strftime("%Y-%m-%d") + '.csv',file=file_object)
#print("Slice Dictionary:","Freeze1_AABC-HCA_Slice_Dictionary_"+ date.today().strftime("%Y-%m-%d") + '.csv',file=file_object)
#print("Slice Univariate Descriptions:","Freeze1_AABC-HCA_Slice_Univariate_Descriptions.txt",file=file_object)
#print("Slice Univariate Plots:","/plots",file=file_object)
#point to new download location which is the same as the slice:
#box = LifespanBox(cache=os.path.join(os.getcwd(),datarequestor))
#if 'Cognition Factor Analysis' in DerivativesRequested:
#    box.downloadFile(config['cogsHCAAABC'])
#    box.downloadFile(1331283608435) #the corresponding readme
#    print("Cognition Factor Analysis: ",os.path.basename(box.downloadFile(config['cogsHCAAABC'])),file=file_object)
# if 'Cardiometabolic Index and Allostatic Load' in DerivativesRequested:
#    box.downloadFile(config['cardiosHCA'])
#    f=box.downloadFile(config['cardiosHCA'])
#    os.rename(f,os.path.join(os.getcwd(),datarequestor,"HCA_Cardiometabolic_Essentials.xlsx"))
#    print("Cardiometabolic Index and Allostatic Load: HCA_Cardiometabolic_Essentials.xlsx",file=file_object)
#    box.downloadFile(1287112770879)  #the Readme
#if 'Vasomotor Symptoms (Processed)' in DerivativesRequested:
#    print("")
#    print("NOT AVAILABLE: Processed Vasomotor Symptom Data", file=file_object)
#if 'Imaging Derived Phenotypes' in DerivativesRequested:
#    print("")
#    print("Imaging Derived Phenotypes for HCA: https://wustl.box.com/s/kohbh1xvh93o35ztns1y8j9nwxw9twqi",file=file_object)

print("", file=file_object)
print("***************************************************************", file=file_object)
print("", file=file_object)
print("Slice created from the following files:",file=file_object)
print("",file=file_object)
for i in y:
    print(i,file=file_object)
print("", file=file_object)
print("", file=file_object)
file_object.close()

#clean up - do not upload the downloaded data into the distribution folder
shutil.rmtree(os.path.join(os.getcwd(),datarequestor+"/downloadedfiles"))

#
#distrib_object = open(os.getcwd()+"/"+datarequestor+"/"+study+"_Slice_Univariate_Descriptions.txt", "w")
#print("Slice_Univariate_Descriptions:",file=distrib_object)
#print("",file=distrib_object)
#for i in [j for j in sliceout.columns if j not in skip_plots]:
#    print("************", file=distrib_object)
#    print("", file=distrib_object)
#    if len(sliceout[i].unique()) >= 25:
#        print("Continuous description "+i+":", file=distrib_object)
#        print("", file=distrib_object)
#        print(sliceout[i].describe(),file=distrib_object)
#        print("", file=distrib_object)
#    if len(sliceout[i].unique()) <25:
#        print("Categorical description for variable "+i+":", file=distrib_object)
#        print("", file=distrib_object)
#        print(sliceout[i].value_counts(dropna=False), file=distrib_object)
#        print("", file=distrib_object)
#print("",file=distrib_object)
#print("***************************************************************",file=distrib_object)
#distrib_object.close()

##################################################################################################
##################################################################################################
