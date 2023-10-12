#TO DO:
# check various combos of instruments and studies
# drop empty columns
# subset to study
# remove housekeeping variables
# include notes variables ??
# map events to harmonized version (e.g. for FU stuff)

##########################################################################
## INSTRUCTIONS

# Create a directory containing this program:
## datarequestor='/your/path'
## make a directory within this folder called
# Download your AABC data (doesn't work on HCA data)... change the datadate and filenames
# Output is a table, slice dictionary, plots, distributions, and receipt

# ###########################################################################
# Currently supports redcap, Q-interactive, ASA24 totals, NIH Toolbox, SSAGA for Visit, and registration, and Covid, Actigraphy events (not FU)
# ###########################################################################


from ccf.box import LifespanBox
import pandas as pd
import matplotlib.pyplot as plt
import os
from datetime import date
from config import *

# Unless you are an administrator, Please create a folder somewhere, such as 'PL_test' below.
# Within that folder, create another folder called 'downloadedfiles'
# Download files from the Pre-Release folder in Box, and move them to the 'downloaded' files directory
# Place a copy of the encyclopedia in that directory, too.

# Now complete the following
datarequestor='/PL_test'  # name of folder in which you want to generate the slice.
study="HCA-AABC" #or HCA

#What instruments?  Please consult the Encyclopedia or check the boxes in the Data portal and use EXACT STRINGs (no extra spaces)
#modify this list:
InstRequested=['MONTREAL COGNITIVE ASSESSMENT (MOCA)',
               'LAB RESULTS',
               'PITTSBURGH SLEEP QUALITY INDEX (PSQI)',
               'NIH Toolbox 4-Meter Walk Gait Speed Test Age 7+ v2.0',
               'NIH Toolbox Oral Reading Recognition Test Age 3+ v2.1',
               'SSAGA - DEMOGRAPHICS','MEMORY FUNCTION QUESTIONNAIRE (MFQ)',
               'SUBJECT REGISTRATION']

#any additional variables?
IndividVars=['neo_n','neo_e']


#Do you want caveman distributions of plots?
wantplots = True  # or False

#Are you an administrator with API credentials in the Pre-Release BOX folder?
isAdmin = False

#if you're not an admin, what is the name of the encyclopedia file you put in the downloadedfiles directory?
encyclopedia_file="AABC_HCA_Encyclopedia_2023-10-03.csv" #if not an admin

#########################################################################################################
################################### Done with user specs #####################

InstRequest1=['SUBJECT INVENTORY AND BASIC INFORMATION']+InstRequested
InstRequest=list(pd.DataFrame(InstRequest1).drop_duplicates()[0])

if not isAdmin:
    E = pd.read_csv(os.getcwd()+datarequestor+'/downloadedfiles/'+encyclopedia_file, low_memory=False, encoding='ISO-8859-1')
    Evars = E.loc[(E['Form / Instrument'].isin(InstRequest)) | (E['Variable / Field Name'].isin(IndividVars))]

if isAdmin:
    try:
        os.mkdir(os.getcwd()+datarequestor)
        os.mkdir(os.getcwd()+datarequestor+'/downloadedfiles/')
    except:
        pass
    try:
        config = LoadSettings()
        secret=pd.read_csv(config['config_files']['secrets'])
        #get list of files in Box folders (interative)
        box = LifespanBox(cache=os.getcwd()+datarequestor+'/downloadedfiles')
        hca_pre=box.list_of_files([str(config['hca_pre'])])
        aabc_pre=box.list_of_files([str(config['aabc_pre'])])
        E=pd.read_csv(box.downloadFile(config['encyclopedia']),low_memory=False,encoding='ISO-8859-1')
        #Evars=E.loc[E['Form / Instrument'].isin(InstRequest)]
        Evars = E.loc[(E['Form / Instrument'].isin(InstRequest)) | (E['Variable / Field Name'].isin(IndividVars))]
        print("Number of Requested Instruments:",len(InstRequest))
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

        aabcfiles=pd.DataFrame.from_dict(aabc_pre, orient='index')
        aabcfiles['datestamp']=aabcfiles.filename.str.split('_',expand=True)[2]
        aabcfiles['datatype']=aabcfiles.filename.str.split('_',expand=True)[1]
        aabcfiles=aabcfiles.loc[(aabcfiles.datestamp.str.contains('.csv')==True) & (aabcfiles.datestamp.str.contains('-'))].copy()
        aabcfiles.datestamp=aabcfiles.datestamp.str.replace('.csv','')
        aabcfiles.datestamp=pd.to_datetime(aabcfiles.datestamp)
        aabcfiles=aabcfiles.sort_values('datestamp',ascending=False)
        aabcfiles=aabcfiles.drop_duplicates(subset='datatype',keep='first').copy()

        #download
        for i in list(aabcfiles.fileid)+list(hcafiles.fileid):
            box.downloadFile(i)
    except:
        print("You do not have API permissions for this request")
##### end Admin section

# Loop through downloaded files and grab any of the Instruments therein, by study.
# THen stack studies together, incorporating exceptions for APOE, FAMILIES, and REgistration variables

widefileAABC=pd.DataFrame(columns=['study','subject','redcap_event'])
widefileHCA=pd.DataFrame(columns=['study','subject','redcap_event'])
for i in os.listdir(os.getcwd()+datarequestor+'/downloadedfiles/'):
    print(i)
    Evars2=list(Evars['Variable / Field Name'])+IndividVars

    if 'NIH' in i and 'Scores' in i:
        bulk=pd.read_csv(os.getcwd()+datarequestor+'/downloadedfiles/'+i,low_memory=False)
        bulk=bulk.loc[bulk.Inst.isin(InstRequest)]
        bulk['subject']=bulk.PIN.str.split('_',expand=True)[0]
        bulk['redcap_event']=bulk.PIN.str.split('_',expand=True)[1]
        tlbxlist=[j for j in InstRequest if 'NIH' in j]
        for k in tlbxlist:
            tlbxvars=E.loc[E['Form / Instrument']==k][['Variable / Field Name','NIH Toolbox Prefix in Slice']]
            tlbxvars['newname']=tlbxvars['NIH Toolbox Prefix in Slice']+'_'+tlbxvars['Variable / Field Name']
            mapvars=dict(zip(list(tlbxvars['Variable / Field Name']),list(tlbxvars.newname)))#,tlbxvars['newname'])
            temp=bulk.loc[bulk.Inst==k][['subject','redcap_event']+list(tlbxvars['Variable / Field Name'])]
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
        temp = pd.read_csv(os.getcwd() + datarequestor + '/downloadedfiles/' + i, low_memory=False)
        subtempA = temp[temp.columns.intersection(set(['subject', 'redcap_event'] + Evars2))]
        dropvars=['pedid','nda_age','nda_interview_date','pseudo_guid','DB_Source','HCAid','HCDid','legacy_yn','sub_event','daysfromtime0','REDCap_id','study_id','psuedo_guid']
        keepvars=[i for i in list(subtempA.columns) if i not in dropvars]
        subtempB = subtempA[keepvars].copy()
        if 'AABC' in i:
            widefileAABC = pd.merge(widefileAABC, subtempB, on=['subject', 'redcap_event'], how='outer')
        if 'HCA' in i:
            widefileHCA = pd.merge(widefileHCA, subtempB, on=['subject', 'redcap_event'], how='outer')

    if 'Inventory' not in i and 'Apoe' not in i and 'Encyclopedia' not in i and 'NIH' not in i and ".DS_Store" not in i and '-INS' not in i and '-Resp' not in i and '-Items' not in i and '-TS' not in i and '-TNS' not in i:
        print(i)
        try:
            temp=pd.read_csv(os.getcwd()+datarequestor+'/downloadedfiles/'+i,low_memory=False)
            subtempA=temp[temp.columns.intersection(set(['subject', 'redcap_event']+Evars2))]
            for j in ['site', 'study', 'PIN', 'redcap_event_name', 'site', 'study_id', 'id', 'gender']:
                try:
                    subtempA = subtempA.drop(columns=[j]).copy()
                except:
                    pass
            if 'AABC' in i:
                widefileAABC=pd.merge(widefileAABC,subtempA, on=['subject','redcap_event'],how='outer')
            if 'HCA' in i:
                widefileHCA = pd.merge(widefileHCA, subtempA, on=['subject', 'redcap_event'], how='outer')
        except:
            print('error')
    if 'Apoe' in i or 'FAMILY' in i:
        temp = pd.read_csv(os.getcwd() + datarequestor + '/downloadedfiles/' + i, low_memory=False)
        subtempA = temp[temp.columns.intersection(set(['subject'] + Evars2))].copy()
        for j in ['site','study', 'PIN', 'redcap_event_name', 'site', 'study_id', 'id', 'gender']:
            try:
                subtempA = subtempA.drop(columns=[j]).copy()
            except:
                pass
        widefileHCA=pd.merge(widefileHCA,subtempA,on='subject',how='outer')

widefileAABC['study'] = 'AABC'
widefileHCA['study'] = 'HCA'
wide=pd.concat([widefileAABC,widefileHCA],axis=0)

#clean up age variable
wide.event_age=wide.event_age.round(1)

#subset by study
if study=="HCA":
    wide=wide.loc[wide.study=="HCA"].copy()

#don't subset by event...drop missing rows
wide['countmiss']=wide.isna().sum(axis=1)
wide=wide.loc[wide.countmiss<(wide.shape[1]-14)]
wide.countmiss.value_counts()
#drop empty columns
wide.dropna(how='all', axis=1, inplace=True)
wide=wide.drop(columns=['countmiss'])


#create output
sliceout = wide[wide.isna().sum(axis=1).ne(wide.shape[1]-3)] #subtracting subject and redcap from total number of missings
sliceout.to_csv(os.getcwd()+datarequestor+"/"+study+"_Slice_"+ date.today().strftime("%Y-%m-%d") + '.csv',index=False)
slicevars=[i for i in list(sliceout.columns) if i not in ['redcap_event','subject','study']]

headerE=E.loc[(E['Variable / Field Name'].isin(['subject','redcap_event'])) & (E['Form / Instrument']=='SUBJECT INVENTORY AND BASIC INFORMATION')]
Evars=Evars.copy()
Evars['newname']=Evars['Variable / Field Name']
Evars.loc[Evars['NIH Toolbox Prefix in Slice'].isnull()==False,'newname']= Evars['NIH Toolbox Prefix in Slice']+'_'+Evars['Variable / Field Name']
D=pd.concat([headerE,Evars.loc[Evars['newname'].isin(slicevars)]])
D=D.drop(columns=['newname'])
D.to_csv(os.getcwd()+datarequestor+"/"+study+"_Slice_Dictionary_"+ date.today().strftime("%Y-%m-%d") + '.csv',index=False)
#    tlbxvars=E.loc[E['Form / Instrument']==k][['Variable / Field Name','NIH Toolbox Prefix in Slice']]
#    tlbxvars['newname']=tlbxvars['NIH Toolbox Prefix in Slice']+'_'+tlbxvars['Variable / Field Name']

#plots:
skip_plots=['subject','redcap_event']
plotlist=[vars for vars in list(sliceout.columns) if vars not in skip_plots]
if wantplots:
    if os.path.exists(os.getcwd()+datarequestor+"/plots"):
        pass
    else:
        os.mkdir(os.getcwd()+datarequestor+"/plots")
    for i in plotlist:
        try:
            sliceout[i].hist()
            plt.savefig(os.getcwd()+datarequestor+"/plots/"+i)#, *, dpi='figure', format=None, metadata=None,
                    #bbox_inches=None, pad_inches=0.1,
                    #facecolor='auto', edgecolor='auto',
                   # backend=None, **kwargs
                   # )
            plt.show()
        except:
            pass


#write receipt
file_object = open(os.getcwd()+datarequestor+"/"+study+"_Slice_Receipt.txt", "w")
print("***************************************************************",file=file_object)
print("",file=file_object)
print("Instruments Requested:",file=file_object)
print("",file=file_object)
for i in InstRequest:
    print(i,file=file_object)
print("",file=file_object)
print("***************************************************************",file=file_object)
print("",file=file_object)
print("Slice:",os.getcwd()+datarequestor+"/"+study+"_Slice_"+ date.today().strftime("%Y-%m-%d") + '.csv',file=file_object)
print("Dictionary:",os.getcwd()+datarequestor+"/"+study+"_Slice_Dictionary_"+ date.today().strftime("%Y-%m-%d") + '.csv',file=file_object)
print("",file=file_object)
print("***************************************************************",file=file_object)
print("",file=file_object)
print("Slice and Dictionary were constructed from one or more of the following snapshot files:",file=file_object)
print("",file=file_object)
for i in os.listdir(os.getcwd()+datarequestor+"/downloadedfiles"):
    if ".DS_Store" != i:
        print(i,file=file_object)
print("",file=file_object)
print("***************************************************************",file=file_object)
print("",file=file_object)
file_object.close()

distrib_object = open(os.getcwd()+datarequestor+"/"+study+"_Variable_Descriptions.txt", "w")
print("Variable Descriptions:",file=distrib_object)
print("",file=distrib_object)
for i in [j for j in sliceout.columns if j !='subject']:
    print("************", file=distrib_object)
    print(sliceout[i].describe(),file=distrib_object)

print("",file=distrib_object)
print("***************************************************************",file=distrib_object)
distrib_object.close()

#specify list of variables: TO DO: extend to other data types grab event age from inventory
# pass empty list if you don't want anything
#redcapvarlist=['sex','legacy_yn','ethnic','racial','site','croms_educ','croms_income','moca_edu','bmi','bp_sitting_systolic','bp_sitting_diastolic','bp_standing_systolic','bp_standing_diastolic','hba1c','hscrp','insulin','vitamind','albumin','alkphos_total','alt_sgpt','ast_sgot','calcium','chloride','co2content','creatinine','glucose','potassium','sodium','totalbilirubin','totalprotein','ureanitrogen','friedewald_ldl','hdl','cholesterol','triglyceride','ldl','estradiol','testosterone','lh','fsh','aldosterone','dheas','cortisol','med1','med2','med3','med4','med5','med6','med7','med8','med9','med10','med11','med12','med13','med14','med15']
#totalsvarlist=['KCAL','MOIS']
#qvarlist=['q_unusable']
# To Do: Cobra (Actigraphy)
# To Do: Inventory - ['event_age']

## other form based variables (examples)
# baselist=['age','sex','ethnic','racial','site','croms_racial','croms_ethnic']
# durel=list(AABCdict.loc[AABCdict['Variable / Field Name'].str.contains('durel')]['Variable / Field Name'])
# formlist=['perceived_everyday_discrimination_block3','neighborhood_disorderneighborhood_social_cohesion','ongoing_chronic_stressors_scale_block3','barriers_to_healthcare_checklist_block3','access_to_healthcare_phenx_block3']
# others=list(AABCdict.loc[AABCdict['Form Name'].isin(formlist)]['Variable / Field Name'])
# red=baselist+gales+durel+others
# redcapvarlist=[i for i in red if "miss" not in i]



##################################################################################################
##################################################################################################
