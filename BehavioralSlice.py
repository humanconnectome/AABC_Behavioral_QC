#TO DO:
# get ssaga in there
# check various combos of instruments and studies
# drop empty columns
# remove housekeeping variables
# include notes variables



# Output is a table and a slice dictionary
# Currently supports redcap, Q-interactive, ASA24 totals, NIH Toolbox, SSAGA for Visit, registration, and Covid, Actigraphy events (not FU)

### PLEASE CHANGE THIS SECTION #####
##########################################################################
## INSTRUCTIONS
# Read request
# Create a directory containing this program:
## datarequestor='/your/path'
## make a directory within this folder called
# Download your AABC data (doesn't work on HCA data)... change the datadate and filenames
# ###########################################################################


from ccf.box import LifespanBox
import pandas as pd
import matplotlib.pyplot as plt
import os
from datetime import date
from config import *

datarequestor='/PL_test'
InstRequest=['MONTREAL COGNITIVE ASSESSMENT (MOCA)','LAB RESULTS', 'PITTSBURGH SLEEP QUALITY INDEX','NIH Toolbox 4-Meter Walk Gait Speed Test Age 7+ v2.0','NIH Toolbox Oral Reading Recognition Test Age 3+ v2.1','SSAGA - DEMOGRAPHICS','MEMORY FUNCTION QUESTIONNAIRE (MFQ)']

slicedate=date.today().strftime("%Y-%m-%d")

## get configuration files
#Petra's shortcuts:
datarequestor='/PL_test'
config = LoadSettings()
secret=pd.read_csv(config['config_files']['secrets'])

try:
    os.mkdir(os.getcwd()+datarequestor)
    os.mkdir(os.getcwd()+datarequestor+'/downloadedfiles/')
except:
    pass

#get list of files in Box folders (interative)
box = LifespanBox(cache=os.getcwd()+datarequestor+'/downloadedfiles')
hca_pre=box.list_of_files([str(config['hca_pre'])])
aabc_pre=box.list_of_files([str(config['aabc_pre'])])
E=pd.read_csv(box.downloadFile(config['encyclopedia']),low_memory=False,encoding='ISO-8859-1')
Evars=E.loc[E['Form / Instrument'].isin(InstRequest)]
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
hcafiles.sort_values('datestamp',ascending=False)
hcafiles=hcafiles.drop_duplicates(subset='datatype',keep='first').copy()

aabcfiles=pd.DataFrame.from_dict(aabc_pre, orient='index')
aabcfiles['datestamp']=aabcfiles.filename.str.split('_',expand=True)[2]
aabcfiles['datatype']=aabcfiles.filename.str.split('_',expand=True)[1]
aabcfiles=aabcfiles.loc[(aabcfiles.datestamp.str.contains('.csv')==True) & (aabcfiles.datestamp.str.contains('-'))].copy()
aabcfiles.datestamp=aabcfiles.datestamp.str.replace('.csv','')
aabcfiles.datestamp=pd.to_datetime(aabcfiles.datestamp)
aabcfiles.sort_values('datestamp',ascending=False)
aabcfiles=aabcfiles.drop_duplicates(subset='datatype',keep='first').copy()

#download
for i in list(aabcfiles.fileid)+list(hcafiles.fileid):
    box.downloadFile(i)

widefileAABC=pd.DataFrame(columns=['study','subject','redcap_event'])
widefileHCA=pd.DataFrame(columns=['study','subject','redcap_event'])
for i in os.listdir(os.getcwd()+datarequestor+'/downloadedfiles/'):
    print(i)
    if "SSAGA" in i:
        SSAGAvars = list(E.loc[E['Form / Instrument'].str.upper().str.contains('SSAGA'), 'Variable / Field Name'])
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
            if 'AABC' in i:
                widefileAABC=pd.merge(widefileAABC,temp, on=['subject','redcap_event'],how='outer')
            if 'HCA' in i:
                widefileHCA = pd.merge(widefileHCA, temp, on=['subject', 'redcap_event'], how='outer')
    if 'SSAGA' not in i and 'Apoe' not in i and 'Encyclopedia' not in i and 'NIH' not in i and ".DS_Store" not in i and '-INS' not in i and '-Resp' not in i and '-Items' not in i and '-TS' not in i and '-TNS' not in i:
        print(i)
        try:
            temp=pd.read_csv(os.getcwd()+datarequestor+'/downloadedfiles/'+i,low_memory=False)
            subtempA=temp[temp.columns.intersection(set(['subject', 'redcap_event']+list(Evars['Variable / Field Name'])))]
            if 'AABC' in i:
                widefileAABC=pd.merge(widefileAABC,subtempA, on=['subject','redcap_event'],how='outer')
            if 'HCA' in i:
                widefileHCA = pd.merge(widefileHCA, subtempA, on=['subject', 'redcap_event'], how='outer')
        except:
            print('error')
        widefileAABC['study'] = 'AABC'
        widefileHCA['study'] = 'HCA'
        wide=pd.concat([widefileAABC,widefileHCA],axis=0)
        wide=wide.loc[wide.redcap_event.isin(['V1','V2','V3','V4'])].copy()
    if 'Apoe' in i:
        temp = pd.read_csv(os.getcwd() + datarequestor + '/downloadedfiles/' + i, low_memory=False)
        subtempA = temp[temp.columns.intersection(set(['subject'] + list(Evars['Variable / Field Name'])))]
        wide=pd.merge(wide,temp,on='subject',how='outer')

sliceout = wide[wide.isna().sum(axis=1).ne(wide.shape[1]-3)] #subtracting subject and redcap from total number of missings
sliceout.to_csv('test.csv',index=False)

#specify list of variables: TO DO: extend to other data types grab event age from inventory
# pass empty list if you don't want anything
study='HCA and AABC'
RedInst=
QInst=
TLBX_inst=

redcapvarlist=['sex','legacy_yn','ethnic','racial','site','croms_educ','croms_income','moca_edu','bmi','bp_sitting_systolic','bp_sitting_diastolic','bp_standing_systolic','bp_standing_diastolic','hba1c','hscrp','insulin','vitamind','albumin','alkphos_total','alt_sgpt','ast_sgot','calcium','chloride','co2content','creatinine','glucose','potassium','sodium','totalbilirubin','totalprotein','ureanitrogen','friedewald_ldl','hdl','cholesterol','triglyceride','ldl','estradiol','testosterone','lh','fsh','aldosterone','dheas','cortisol','med1','med2','med3','med4','med5','med6','med7','med8','med9','med10','med11','med12','med13','med14','med15']
totalsvarlist=['KCAL','MOIS']
qvarlist=['q_unusable']
tlbx_scores: NIHTLBX_scores
# To Do: Cobra (Actigraphy)
# To Do: Inventory - ['event_age']

## other form based variables (examples)
# baselist=['age','sex','ethnic','racial','site','croms_racial','croms_ethnic']
# durel=list(AABCdict.loc[AABCdict['Variable / Field Name'].str.contains('durel')]['Variable / Field Name'])
# formlist=['perceived_everyday_discrimination_block3','neighborhood_disorderneighborhood_social_cohesion','ongoing_chronic_stressors_scale_block3','barriers_to_healthcare_checklist_block3','access_to_healthcare_phenx_block3']
# others=list(AABCdict.loc[AABCdict['Form Name'].isin(formlist)]['Variable / Field Name'])
# red=baselist+gales+durel+others
# redcapvarlist=[i for i in red if "miss" not in i]

#Do you want caveman distribution plots?
wantplots=True  # or False


##################################################################################################
##################################################################################################
# Leave the rest of this code alone unless you want to add in other merges ######################
# All files have 'subject' and 'redcap_event' for this purpose but ###############################

# read the aabc dictionary to grab list of registration variables
AABCdict=pd.read_csv(tmpdir+aabcdictionary,low_memory=False)
AABCdict.loc[AABCdict['Variable / Field Name'].isin(redcapvarlist)].to_csv(savefiles+'Slice_Dictionary.csv',index=False)

# separate REDCap variables into those collected at registration and those collected any other time
RegisterVars=list(AABCdict.loc[AABCdict['Form Name']=='register_subject','Variable / Field Name'])
inreg=[vars for vars in redcapvarlist if vars in RegisterVars]
notinreg=[vars for vars in redcapvarlist if vars not in RegisterVars]

# NOW GET THE DATA
Totals=pd.read_csv(tmpdir+'AABC_ASA24-Totals_'+datadate+'.csv',low_memory=False)[['subject','redcap_event']+totalsvarlist]
Qdata=pd.read_csv(tmpdir+'AABC_Q-Interactive_'+datadate+'.csv',low_memory=False)[['subject','redcap_event']+qvarlist]
REDCapReg=pd.read_csv(tmpdir+'AABC_REDCap_'+datadate+'.csv',low_memory=False)[['subject','redcap_event']+inreg]
REDCapReg=REDCapReg.loc[REDCapReg.redcap_event.str.contains('AF0')].drop(columns=['redcap_event'])
REDCapVisit=pd.read_csv(tmpdir+'AABC_REDCap_'+datadate+'.csv',low_memory=False)[['subject','redcap_event']+notinreg]
REDCapVisit=REDCapVisit.loc[REDCapVisit.redcap_event.str.contains('V')]

# merge REDCap parts. Eventually will be multiple visits per person and this will grab HCA data, too.
REDCap=pd.merge(REDCapReg.loc[~(REDCapReg.subject=='')],REDCapVisit,on='subject',how='left')
Alldata_a=pd.merge(REDCap,Totals,on=['subject','redcap_event'],how='left')
Alldata=pd.merge(Alldata_a,Qdata,on=['subject','redcap_event'],how='left')

# CHECK FILTER SO THAT ONLY FOLKS WHO HAVE REGISTERED FOR A VISIT GET SENT TO PRE-RELEASE FOLDERS

#reorder
rightcols=[col for col in Alldata.columns if (col != "redcap_event" and col !="subject")]
#create csv
Alldata[['subject','redcap_event']+rightcols].to_csv(savefiles+"AABC_Behavioral_Data_Slice_"+datadate+'.csv',index=False)

# super basic plots of variable distributions
skip_plots=['subject','redcap_event']
plotlist=[vars for vars in list(Alldata.columns) if vars not in skip_plots]

if wantplots:
    if os.path.exists(savefiles+"plots"):
        pass
    else:
        os.mkdir(savefiles+"plots")
    for i in plotlist:
        try:
            Alldata.hist(column=i)
            plt.savefig(savefiles+"plots/"+i)#, *, dpi='figure', format=None, metadata=None,
                    #bbox_inches=None, pad_inches=0.1,
                    #facecolor='auto', edgecolor='auto',
                   # backend=None, **kwargs
                   # )
            plt.show()
        except:
            pass

# now merge data dictionaries - code to be used later when harmony requested
wantdicts=False
if wantdicts:
    #load the dictionaries (again) except ssaga, which is in a completely separate data base for HCA
    AABCdict=pd.read_csv(tmpdir+aabcdictionary,low_memory=False)
    AABCdict=AABCdict.loc[~(AABCdict['Form Name']=='ssaga')]
    AABCdict=AABCdict.loc[~(AABCdict['Variable / Field Name'].str.contains('miss') & (AABCdict['Field Type']=='descriptive'))]

    #create a variable order so that you can keep track of when the variables show up
    AABCdict.reset_index(inplace=True)#.rename(columns={'index':'Variable Order'})
    AABCdict['Variable Order AABC']=AABCdict['index']  #.rename(columns={'index','Variable Order'})

    HCAdict=pd.read_csv(tmpdir+hcadatadictionary,low_memory=False)
    HCAdict=HCAdict.loc[~(HCAdict['Variable / Field Name'].str.contains('miss') & (HCAdict['Field Type']=='descriptive'))]
    HCAdict.reset_index(inplace=True)#.rename(columns={'index':'Variable Order'})
    HCAdict['Variable Order HCA']=HCAdict['index']  #.rename(columns={'index','Variable Order'})

    keeplist=['Field Annotation','Variable / Field Name', 'Form Name', 'Section Header','Field Type', 'Field Label', 'Choices, Calculations, OR Slider Labels','Field Note', 'Branching Logic (Show field only if...)']
    together=pd.merge(AABCdict[keeplist+['Variable Order AABC']],HCAdict[keeplist+['Variable Order HCA']],on='Variable / Field Name',how='outer',indicator=True)
    d={"left_only":"Only present in AABC", "right_only":"Only present in HCA","both":"Present in Both AABC and HCA"}
    together['_merge'] = together['_merge'].map(d)
    together.to_csv(savefiles+"AABC_and_HCA_REDCap_DataDictionaries.csv",index=False)

