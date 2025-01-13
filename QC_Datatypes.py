# %% [markdown]
# # AABC cumulative recruitment stats and visuals
# 

# %%
# check out  ~/cron/aabc_recruits.sh
#

# %%
#load some libraries
import pandas as pd
import seaborn as sns
from ccf.box import LifespanBox
import yaml
from functions import *
from config import *
import numpy as np
import matplotlib.pyplot as plt
from datetime import date
import warnings
from itables import show
import itables.options as opt
warnings.filterwarnings('ignore')

# %%
opt.maxBytes = "512KB"

# %%
print(date.today().strftime("%m/%d/%Y"))

# %%
#load HCA inventory 
config = LoadSettings()
secret=pd.read_csv(config['config_files']['secrets'])
box = LifespanBox(cache="./tmp")
pathp=box.downloadFile(config['hcainventory'])
ids=pd.read_csv(pathp)

# %%
## get configuration files
intradb=pd.read_csv(config['config_files']['PCP'])
#aabc_processing=config['aabc_processing']

# %%
DNR = ["HCA7787304_V1", "HCA6276071_V1", "HCA6229365_V1", "HCA9191078_V1", "HCA6863086_V1"]
#These guys accidentally recruited as V2
v2oops=['HCA6686191','HCA7296183']

# %%
aabcdictionary='AABC_REDCap_DataDictionary_2023-05-15.csv'                 # aabc REDCap data dictionary...necessary for automating variables at appropriate events - put in tmpdir
#AABCdict=pd.read_csv(outp+aabcdictionary,low_memory=False)
E=pd.read_csv(box.downloadFile(config['encyclopedia']),low_memory=False,encoding='ISO-8859-1')
SSAGAvars=list(E.loc[E['Form / Instrument'].str.upper().str.contains('SSAGA'),'Variable / Field Name'])

# %%
hcaids=ids.subject.drop_duplicates()
#for later use in getting the last visit for each participant in HCA so that you can later make sure that person is starting subsequent visit and not accidentally enrolled in the wrong arm
hca_lastvisits=ids[['subject','redcap_event']].loc[ids.redcap_event.isin(['V1','V2'])].sort_values('redcap_event').drop_duplicates(subset='subject',keep='last')

# %%
#load AABC report
aabcarms = redjson(tok=secret.loc[secret.source=='aabcarms','api_key'].reset_index().drop(columns='index').api_key[0])
hcpa = redjson(tok=secret.loc[secret.source=='hcpa','api_key'].reset_index().drop(columns='index').api_key[0])
#just a report
aabcreport = redreport(tok=secret.loc[secret.source=='aabcarms','api_key'].reset_index().drop(columns='index').api_key[0],reportid='51031')


# %%

#download the inventory report from AABC for comparison
aabcinvent=getframe(struct=aabcreport,api_url=config['Redcap']['api_url10']).drop(columns='dob')


# %% [markdown]
# ## CODE RED!

# %%
#find subjectts who have completed a visit and are not DNR - save for later
reds=aabcinvent.loc[aabcinvent.register_visit_complete =='2'][['study_id']]
inperson=list(reds.study_id.unique())
reds2=aabcinvent.loc[(aabcinvent.study_id.isin(inperson)) & (~(aabcinvent.subject_id =='')) & (~(aabcinvent.subject_id.isin(DNR)))]
inpersonHCAid=list(reds2.subject_id.unique())

# %%
#trying to set study_id from config file, but have been sloppy...there are instances where the actual subject_id has been coded below
study_id=config['Redcap']['datasources']['aabcarms']['redcapidvar']

#slim selects just the registration event (V0) because thats where the ids and legacy information is kept.
slim=aabcinvent[['study_id','redcap_event_name',study_id,'legacy_yn','site','v0_date']].loc[(aabcinvent.redcap_event_name.str.contains('register'))]

#compare aabc ids against hcaids and whether legacy information is properly accounted for (e.g. legacy variable flags and actual event in which participannt has been enrolled.
fortest=pd.merge(hcaids,slim,left_on='subject',right_on=study_id,how="outer",indicator=True)
#fortest._merge.value_counts()
legacyarms=['register_arm_1','register_arm_2','register_arm_3','register_arm_4','register_arm_5','register_arm_6','register_arm_7','register_arm_8']

# %%
# First batch of flags: Look for legacy IDs that don't actually exist in HCA
ft=fortest.loc[(fortest._merge=='right_only') & ((fortest.legacy_yn=='1')|(fortest.redcap_event_name.isin(legacyarms)))]
#remove the TEST subjects -- probably better to do this first, but sigh.
ft=ft.loc[~((ft[study_id]=='')|(ft[study_id].str.upper().str.contains('TEST')))]
qlist1=pd.DataFrame()
if not ft.empty:
    ft['reason']='Subject found in AABC REDCap Database with legacy indications whose ID was not found in HCP-A list'
    ft['issueCode']='AE1001'
    ft['datatype']='REDCap'
    ft['code']='RED'
    qlist1=ft[['subject_id', 'study_id', 'redcap_event_name', 'site','reason','code','v0_date','datatype']]
    for s in list(ft[study_id].unique()):
        print('CODE RED :',s,': Subject found in AABC REDCap Database with legacy indications whose ID was not found in HCP-A list')

#2nd batch of flags: if legacy v1 and enrolled as if v3 or v4 or legacy v2 and enrolled v4
ft2=fortest.loc[(fortest._merge=='both') & ((fortest.legacy_yn != '1')|(~(fortest.redcap_event_name.isin(legacyarms))))]
qlist2=pd.DataFrame()
if not ft2.empty:
    ft2['reason']='Subject found in AABC REDCap Database with legacy indications whose ID was not found in HCP-A list'
    ft2['code']='RED'
    ft2['issueCode'] = 'AE1001'
    ft2['datatype']='REDCap'
    qlist2 = ft2[['subject_id', 'study_id', 'redcap_event_name', 'site','reason','code','v0_date','datatype']]
    for s2 in list(ft2[study_id].unique()):
        print('CODE RED :',s2,': Subject found in AABC REDCap Database with an ID from HCP-A study but no legacyYN not checked')



# %%
# Check if subject fail the screen but came in for visit
pass_failed=aabcinvent.loc[(aabcinvent.passedscreen =='2') & (aabcinvent['subject_id'].astype(str).str.strip() != '')][['subject_id', 'study_id', 'redcap_event_name', 'site','v0_date']]
qlist4=pd.DataFrame()
if not pass_failed.empty:
    pass_failed['reason']='subject did not pass screen but came in for imaging - need confirmation'
    pass_failed['code']='RED'
    pass_failed['issueCode'] = 'AE1001'
    pass_failed['datatype']='REDCap'
    qlist4 = pass_failed[['subject_id', 'study_id', 'redcap_event_name', 'site','reason','code','v0_date','datatype']]
    for s4 in list(pass_failed[study_id].unique()):
        print('CODE RED :',s4,': subject did not pass screen but came in for imaging - need confirmation')

# %%
#if legacy v1 and enrolled as if v3 or v4 or legacy v2 and enrolled v4
#get last visit
hca_lastvisits["next_visit"]=''
#idvisits rolls out the subject ids to all visits. get subects current visit for comparison with last visit
aabcidvisits=idvisits(aabcinvent,keepsies=['study_id','redcap_event_name','site','subject_id','v0_date','event_date'])
sortaabc=aabcidvisits.sort_values(['study_id','redcap_event_name'])
sortaabcv=sortaabc.loc[~(sortaabc.redcap_event_name.str.contains('register'))]
sortaabcv.drop_duplicates(subset=['study_id'],keep='first')
#print("OOOPSs:",sortaabcv.loc[sortaabcv.subject.isin(v2oops)])
#add 1 to last visit from HCA
#also set up for checking to make sure not initiating same visit
hca_lastvisits.next_visit=hca_lastvisits.redcap_event.str.replace('V','').astype('int') +1
hca_lastvisits["next_visit2"]="V"+hca_lastvisits.next_visit.astype(str)
hca_lastvisits2=hca_lastvisits.drop(columns=['redcap_event','next_visit'])

# %%
#check that current visit in AABC is the last visit in HCA + 1
check=pd.merge(hca_lastvisits2,sortaabcv,left_on=['subject','next_visit2'],right_on=['subject','redcap_event'],how='outer',indicator=True)
check=check.loc[check._merge !='left_only']
wrongvisit=check.loc[check._merge=='right_only']
wrongvisit=wrongvisit.loc[~(wrongvisit.redcap_event.isin(['AP']))]#,'v1_inperson_arm_10','v1_inperson_arm_12']))]
wrongvisit=wrongvisit.loc[wrongvisit.next_visit2.isnull()==False]

# %%
qlist3=pd.DataFrame()
if not wrongvisit.empty:
    wrongvisit['reason']='Subject found in AABC REDCap Database initiating the wrong visit sequence (e.g. V3 insteady of V2'
    wrongvisit['code']='RED'
    wrongvisit['issueCode'] = 'AE1001'
    wrongvisit['datatype']='REDCap'
    qlist3 = wrongvisit[['subject', 'study_id', 'redcap_event_name', 'site','reason','code','v0_date','event_date','datatype']]
    qlist3=qlist3.rename(columns={'subject':'subject_id'})
    for s3 in list(wrongvisit['subject'].unique()):
        if s3 !='':
            print('CODE RED (if HCA6911778 ignore) :',s3,': Subject found in AABC REDCap Database initiating the wrong visit sequence (e.g. V3 insteady of V2')
            qlist3=qlist3.loc[~(qlist3.subject_id=='HCA6911778')].copy()

# %%
#check to make sure they aren't initiating the same visit.
check2=pd.merge(hca_lastvisits[['subject','redcap_event']],sortaabcv,left_on=['subject','redcap_event'],right_on=['subject','redcap_event'],how='inner')
check2=check2.loc[~(check2.subject.isin(v2oops))]
qlist32=pd.DataFrame()
if not check2.empty:
    check2['reason']='Subject found in AABC REDCap Database initiating the wrong visit sequence (e.g. V3 insteady of V2'
    check2['code']='RED'
    check2['issueCode'] = 'AE1001'
    check2['datatype']='REDCap'
    qlist32 = check2[['subject', 'study_id', 'redcap_event_name', 'site','reason','code','v0_date','event_date','datatype']]
    qlist32=qlist32.rename(columns={'subject':'subject_id'})
    for s3 in list(check2['subject'].unique()):
        if s3 !='':
            print('CODE RED (if HCA6911778 ignore) :',s3,': Subject found in AABC REDCap Database initiating the wrong visit sequence (e.g. V3 insteady of V2')
            qlist32=qlist32.loc[~(qlist32.subject_id=='HCA6911778')].copy()

# %%
#test subjects that need to be deleted
tests=aabcinvent.loc[(aabcinvent[study_id].str.upper().str.contains('TEST')) | (aabcinvent[study_id].str.upper().str.contains('PRAC')) | (aabcinvent[study_id].str.upper().str.contains('DEMO'))][['study_id',study_id,'redcap_event_name']]
qlist5=pd.DataFrame()
if not tests.empty:
    tests['reason']='HOUSEKEEPING : Please delete test subject.  Use test database when practicing'
    tests['code']='HOUSEKEEPING'
    tests['datatype']='REDCap'
    tests['issueCode'] = 'AE6001'
    qlist5 = tests[['subject_id', 'study_id', 'redcap_event_name', 'site','reason','code','v0_date','event_date','datatype']]
    for s5 in list(tests[study_id].unique()):
        print('HOUSEKEEPING : Please delete test subject:', s5)

# %% [markdown]
# ## End of CODE RED

# %%
### PPPPP
#CLEAN UP OPTION FOR PRODUCING PLOTS BY VISIT instead of SUBJECT


#api metadata
aabcarms = redjson(tok=secret.loc[secret.source=='aabcarms','api_key'].reset_index().drop(columns='index').api_key[0])
hcpa = redjson(tok=secret.loc[secret.source=='hcpa','api_key'].reset_index().drop(columns='index').api_key[0])

#report deets
aabcreport = redreport(tok=secret.loc[secret.source=='aabcarms','api_key'].reset_index().drop(columns='index').api_key[0],reportid='51031')

#download the inventory report from AABC
#aabcinvent=getframe(struct=aabcreport,api_url=config['Redcap']['api_url10']).drop(columns='dob')
print("shape of inventory before filters or merges",aabcinvent.shape)

#roll up variables that only exist in registration so that you can produce stats by visits
aabcinvent=aabcinvent.drop(columns=['subject_id','site','sex','ethnic','racial','croms_income','counterbalance_1st', 'passedscreen']).merge(aabcinvent.loc[~(aabcinvent.subject_id =='')][['study_id','subject_id','site','sex','ethnic','racial','croms_income','counterbalance_1st', 'passedscreen']],on='study_id',how='left')

#print("double check shape of inventory before filters:",aabcinvent.shape)
print("number of unique subjects before filters (includes 1 row for a blank):",str(len(aabcinvent.subject_id.unique())))
print("number of unique REDCap IDs:",str(len(aabcinvent.study_id.unique())))

#passed screen 
aabcinvent = aabcinvent.loc[aabcinvent.passedscreen == '1'] 
print("shape of inventory passed screen",aabcinvent.shape)


# %%

aabcinvent['todaydate']=date.today()
#BAD formula:
#aabcinvent['dayspassed']=(pd.to_datetime(aabcinvent.todaydate) - pd.to_datetime(aabcinvent.event_date)).dt.days

#GOOD FORMULA:
aabcinvent['dayspassed']=(pd.to_datetime(aabcinvent.event_date)- pd.to_datetime('05/22/2022')).dt.days
aabcinvent=aabcinvent.sort_values('dayspassed')
#print('data shape: ', str(aabcinvent.shape))
##print('selecting visits with completed registration (indicates they actually came in)')
#accrued=aabcinvent.loc[(aabcinvent.register_visit_complete=='2') & (aabcinvent.redcap_event_name.str.contains('inperson'))]
accrued = aabcinvent.loc[aabcinvent.redcap_event_name.str.contains('inperson')].drop_duplicates(subset='subject_id', keep='first')
#print('select first in-person visit: ', str(accrued.shape))
accrued = accrued.loc[accrued.register_visit_complete == '2'] 
#print('select visits with completed registration: ', str(accrued.shape))
#print('now the shape of the data is:',str(accrued.shape))

#uncomment if you want to get the event counts
#print(accrued.redcap_event_name.value_counts())


# %%
print("number of unique subjects now:",str(len(accrued.subject_id.unique())))

# %%
#droplist=['HCA7787304','HCA7142156','HCA6863086','HCA6276071','HCA6229365','HCA9191078']
droplist=['HCA7787304','HCA7142156','HCA6863086','HCA6276071','HCA6229365','HCA9191078', 'HCA8743995','HCA62276071',
          'HCA6418974', 'HCA9841899', 'HCA6298788', 'HCA9515381']
accruedclean=accrued.loc[~(accrued.subject_id.isin(droplist))]
accruedclean=accruedclean.loc[~(accruedclean.passedscreen.isna())]
print('drop any stragglers that failed screening or withdrew')
print(accruedclean.shape)

#select subjects visit before Oct 1
#accruedclean = accruedclean[accruedclean['event_date'] < '2024-10-01']
#print(accruedclean.shape)

forplot=accruedclean.copy()

#use age at visit (age_visit) instead of age at baseline (age).
forplot=forplot.drop(columns=['age']).rename(columns={'age_visit':'age'})
#print(list(forplot.columns))


# %%
#'HCA7142156','HCA6276071','HCA7787304','HCA6863086','HCA9191078','HCA6229365','HCA8743995'

# %%
#RR_subjects = forplot[forplot['site'] == '4']
#RR_subjects = pd.DataFrame(RR_subjects['subject_id'])
#print(RR_subjects.shape)
#master = pd.read_excel('WU_Master_log.xlsx')
#master=master.loc[~(master.subject_id.isin(droplist))]
#print(master.shape)

# %%
# Compare the column from both dataframes
#differences = master[~master['subject_id'].isin(RR_subjects['subject_id'])]
#print("Differences in master not in RR_subjects:")
#print(differences)

# %%
# Compare the column from both dataframes
#differences2 = RR_subjects[~RR_subjects['subject_id'].isin(master['subject_id'])]
#print("Differences in RR_subjects not in master:")
#print(differences2)

# %%
forplot['croms_income'] = forplot['croms_income'].apply(clean_croms_income)

# %%
#PREPARE DATA FOR CUMULATIVE PLOTS  
S=pd.get_dummies(forplot.sex, prefix='sex')

#forplot['sexsum']=pd.to_numeric(forplot.sex, errors='coerce').cumsum()
forplot['malesum']=pd.to_numeric(S.sex_1, errors='coerce').cumsum()
forplot['femalesum']=pd.to_numeric(S.sex_2, errors='coerce').cumsum()
forplot['Sex']=forplot.sex.replace({'1':'Male','2':'Female'})

S0=pd.get_dummies(forplot.counterbalance_1st, prefix='CB')
forplot['CB3sum']=pd.to_numeric(S0.CB_3, errors='coerce').cumsum()
forplot['CB4sum']=pd.to_numeric(S0.CB_4, errors='coerce').cumsum()
forplot['Counterbalance']=forplot.counterbalance_1st.replace({'3':'CB3','4':'CB4'})

#forplot[['subject_id','dayspassed','malesum','femalesum','Sex']].head(20)

# %%
#forplot.redcap_event_name.value_counts()
forplot['Cohort']=''
forplot.loc[(forplot.redcap_event_name.str.contains("arm_1")) | (forplot.redcap_event_name.str.contains("arm_2")) |(forplot.redcap_event_name.str.contains("arm_3")) |(forplot.redcap_event_name.str.contains("arm_4")) ,'Cohort']='Cohort A'
forplot.loc[(forplot.redcap_event_name.str.contains("arm_5")) | (forplot.redcap_event_name.str.contains("arm_6")) |(forplot.redcap_event_name.str.contains("arm_7")) |(forplot.redcap_event_name.str.contains("arm_8")) ,'Cohort']='Cohort B'
forplot.loc[(forplot.redcap_event_name.str.contains("arm_9")) | (forplot.redcap_event_name.str.contains("arm_10")) |(forplot.redcap_event_name.str.contains("arm_11")) |(forplot.redcap_event_name.str.contains("arm_12")) ,'Cohort']='Cohort C'
#forplot.Cohort

# %%
#1, Native American/Alaskan Native | 2, Asian | 3, Black or African American | 4, Native Hawaiian or Other Pacific Is | 5, White | 6, More than one race | 99, Unknown or Not reported
S2=pd.get_dummies(forplot.racial, prefix='race')
#print(S2.head())
forplot['whitesum']=pd.to_numeric(S2.race_5, errors='coerce').cumsum()
#forplot['natpacsum']=pd.to_numeric(S2.race_4, errors='coerce').cumsum()
forplot['blacksum']=pd.to_numeric(S2.race_3, errors='coerce').cumsum()
forplot['asiansum']=pd.to_numeric(S2.race_2, errors='coerce').cumsum()
forplot['natamersum']=pd.to_numeric(S2.race_1, errors='coerce').cumsum()
forplot['moret1sum']=pd.to_numeric(S2.race_6, errors='coerce').cumsum()
forplot['nasum']=pd.to_numeric(S2.race_99, errors='coerce').cumsum()
forplot['Race']=forplot.racial.replace({'1':'Nat Amer/Alaskan','2':'Asian','3':'Black','4':'Nat Hawaiian/PI','5':'White','6':'More than one','99':'Unknown'})

#thnicity
S3=pd.get_dummies(forplot.ethnic, prefix='ethnicity')
forplot['hispanicsum']=pd.to_numeric(S3.ethnicity_1, errors='coerce').cumsum()
forplot['nonhispanicsum']=pd.to_numeric(S3.ethnicity_2, errors='coerce').cumsum()
forplot['unkhispsum']=pd.to_numeric(S3.ethnicity_3, errors='coerce').cumsum()
forplot['Ethnicity']=forplot.ethnic.replace({'1':'Hispanic','2':'Non-Hispanic','3':'Unknown'})

#sites
S4=pd.get_dummies(forplot.site, prefix='site')
forplot['wusum']=pd.to_numeric(S4.site_4, errors='coerce').cumsum()
forplot['umnsum']=pd.to_numeric(S4.site_3, errors='coerce').cumsum()
forplot['mghsum']=pd.to_numeric(S4.site_1, errors='coerce').cumsum()
forplot['uclasum']=pd.to_numeric(S4.site_2, errors='coerce').cumsum()
forplot['Site']=forplot.site.replace({'1':'MGH','2':'UCLA','3':'UMN','4':'WashU'})



# %%
#forplot.head(20)

# %%
                                       
##ages
bins= [30,40,50,60,70,80,90,125]

forplot['ages']=pd.to_numeric(forplot.age)
forplot['AgeGroup'] = pd.cut(forplot['ages'], bins=bins,right=False)# labels=labels,
S5=pd.get_dummies(forplot.AgeGroup, prefix='age')

forplot['age30sum']=pd.to_numeric(S5['age_[30, 40)'], errors='coerce').cumsum()
forplot['age40sum']=pd.to_numeric(S5['age_[40, 50)'], errors='coerce').cumsum()
forplot['age50sum']=pd.to_numeric(S5['age_[50, 60)'], errors='coerce').cumsum()
forplot['age60sum']=pd.to_numeric(S5['age_[60, 70)'], errors='coerce').cumsum()
forplot['age70sum']=pd.to_numeric(S5['age_[70, 80)'], errors='coerce').cumsum()
forplot['age80sum']=pd.to_numeric(S5['age_[80, 90)'], errors='coerce').cumsum()
forplot['age90sum']=pd.to_numeric(S5['age_[90, 125)'],errors='coerce').cumsum()

# %%
bins5= [35,40,45,50,55,60,65,70,75,80,85,90,125]

forplot['AgeGroup5'] = pd.cut(forplot['ages'], bins=bins5,right=False)# labels=labels,
S55=pd.get_dummies(forplot.AgeGroup5, prefix='age5')

forplot['age35sum5']=pd.to_numeric(S55['age5_[35, 40)'], errors='coerce').cumsum()
forplot['age40sum5']=pd.to_numeric(S55['age5_[40, 45)'], errors='coerce').cumsum()
forplot['age45sum5']=pd.to_numeric(S55['age5_[45, 50)'], errors='coerce').cumsum()
forplot['age50sum5']=pd.to_numeric(S55['age5_[50, 55)'], errors='coerce').cumsum()
forplot['age55sum5']=pd.to_numeric(S55['age5_[55, 60)'], errors='coerce').cumsum()
forplot['age60sum5']=pd.to_numeric(S55['age5_[60, 65)'], errors='coerce').cumsum()
forplot['age65sum5']=pd.to_numeric(S55['age5_[65, 70)'], errors='coerce').cumsum()
forplot['age70sum5']=pd.to_numeric(S55['age5_[70, 75)'], errors='coerce').cumsum()
forplot['age75sum5']=pd.to_numeric(S55['age5_[75, 80)'],errors='coerce').cumsum()
forplot['age80sum5']=pd.to_numeric(S55['age5_[80, 85)'],errors='coerce').cumsum()
forplot['age85sum5']=pd.to_numeric(S55['age5_[85, 90)'],errors='coerce').cumsum()
forplot['age90sum5']=pd.to_numeric(S55['age5_[90, 125)'],errors='coerce').cumsum()

#forplot.columns

# %%
##ages
bins60= [0,60,125]

forplot['AgeGroup60'] = pd.cut(forplot['ages'], bins=bins60,right=False)# labels=labels,
S60=pd.get_dummies(forplot.AgeGroup60, prefix='age60')

forplot['age1sum60']=pd.to_numeric(S60['age60_[0, 60)'], errors='coerce').cumsum()
forplot['age2sum60']=pd.to_numeric(S60['age60_[60, 125)'],errors='coerce').cumsum()

# %%
## croms income
bins_income= [0,20000,50000,100000, np.inf]
labels_income = ['0-20k', '20k-50k', '50k-100k', '100k+']

forplot['income']=pd.to_numeric(forplot.croms_income)
forplot['IncomeGroup'] = pd.cut(forplot['income'], bins=bins_income,labels=labels_income,right=False)# labels=labels,
forplot['IncomeGroup'] = forplot['IncomeGroup'].cat.add_categories('Don\'t know').fillna('Don\'t know')

S6=pd.get_dummies(forplot.IncomeGroup, prefix='income')


# %%
## education level
bins_edu = [0, 13, 15, 77, np.inf]
labels_edu = ['Below Highschool', 'Highschool or GED', 'Above Highschool or GED', 'Refused or do not know']

forplot['moca_edu']=pd.to_numeric(forplot.moca_edu)
forplot['EduGroup'] = pd.cut(forplot['moca_edu'], bins=bins_edu,labels=labels_edu,right=False)# labels=labels,
forplot['EduGroup'] = forplot['EduGroup'].cat.add_categories('Don\'t know').fillna('Don\'t know')
S6=pd.get_dummies(forplot.EduGroup, prefix='moca_edu')

# %% [markdown]
# ## ALL participants, by cohort

# %%
#Cohort x Age
pd.crosstab(forplot.Cohort,forplot.AgeGroup5,margins=True)#.plot.bar(rot=45,title='AABC')

# %%
## Cohort x sex
pd.crosstab(forplot.Cohort,forplot.Sex,margins=True)#.plot.bar(rot=45,title='AABC')

# %%
## Cohort x race
pd.crosstab(forplot.Cohort,forplot.Race,margins=True)#.plot.bar(rot=45,title='AABC')

# %%
## Cohort x ethnicity
pd.crosstab(forplot.Cohort,forplot.Ethnicity,margins=True)#.plot.bar(rot=45,title='AABC')

# %%
#Cohort x Income
pd.crosstab(forplot.Cohort,forplot.IncomeGroup,margins=True)#.plot.bar(rot=45,title='AABC')

# %%
#Cohort x Education
pd.crosstab(forplot.Cohort,forplot.EduGroup,margins=True)#.plot.bar(rot=45,title='AABC')

# %% [markdown]
# ### AABC Female participants, by age and cohort 

# %%
#make a table by cohort, 5-year age
#FeMALES
females=forplot.loc[forplot.Sex=='Female'].copy()
pd.crosstab(females.Cohort,females.AgeGroup5,margins=True)#.plot.bar(rot=45,title='AABC')

# %% [markdown]
# ### AABC Male participants, by age and cohort 

# %%
#make a table by cohort, 5-year age
males=forplot.loc[forplot.Sex=='Male'].copy()
pd.crosstab(males.Cohort,males.AgeGroup5,margins=True)#.plot.bar(rot=45,title='AABC')

# %% [markdown]
# ### AABC Female participants, by race and cohort

# %%
#make a table by cohort, race
#FeMALES
females=forplot.loc[forplot.Sex=='Female'].copy()
pd.crosstab(females.Cohort,females.Race,margins=True)#.plot.bar(rot=45,title='AABC')

# %% [markdown]
# ### AABC Male participants, by race and cohort 

# %%
#make a table by cohort, race
#FeMALES
males=forplot.loc[forplot.Sex=='Male'].copy()
pd.crosstab(males.Cohort,males.Race,margins=True)#.plot.bar(rot=45,title='AABC')

# %% [markdown]
# ### AABC Female participants, by ethnicity and cohort

# %%
#make a table by cohort, ethnicity
#FeMALES
females=forplot.loc[forplot.Sex=='Female'].copy()
pd.crosstab(females.Cohort,females.Ethnicity,margins=True)#.plot.bar(rot=45,title='AABC')

# %% [markdown]
# ### AABC Male participants, by ethnicity and cohort

# %%
#make a table by cohort, ethnicity
#FeMALES
males=forplot.loc[forplot.Sex=='Male'].copy()
pd.crosstab(males.Cohort,males.Ethnicity,margins=True)#.plot.bar(rot=45,title='AABC')

# %% [markdown]
# ### Sex stats, by income and education

# %%
#make a table by sex, income and education level
pd.crosstab([forplot['Sex']], [forplot['IncomeGroup'], forplot['EduGroup']], margins=True)

# %% [markdown]
# ### Race stats, by income and education

# %%
#make a table by race, income and education level
pd.crosstab([forplot['Race']], [forplot['IncomeGroup'], forplot['EduGroup']], margins=True)

# %% [markdown]
# ### Site stats , by age and cohort

# %%
#make a table by cohort, 5-year age
print("*******************")
print("**** MGH ONLY *****")
mgh=forplot.loc[forplot.Site=='MGH'].copy()
pd.crosstab(mgh.Cohort,mgh.AgeGroup5,margins=True)#.plot.bar(rot=45,title='AABC')

# %%
#make a table by cohort, ethnicity
mgh=forplot.loc[forplot.Site=='MGH'].copy()
pd.crosstab(mgh.Cohort,mgh.Ethnicity,margins=True)#.plot.bar(rot=45,title='AABC')

# %%
#make a table by cohort, race
mgh=forplot.loc[forplot.Site=='MGH'].copy()
pd.crosstab(mgh.Cohort,mgh.Race,margins=True)#.plot.bar(rot=45,title='AABC')

# %%
#make a table by cohort, 5-year age
print("*******************")
print("**** UCLA ONLY *****")
ucla=forplot.loc[forplot.Site=='UCLA'].copy()
pd.crosstab(ucla.Cohort,ucla.AgeGroup5,margins=True)#.plot.bar(rot=45,title='AABC')

# %%
#make a table by cohort, ethnicity
ucla=forplot.loc[forplot.Site=='UCLA'].copy()
pd.crosstab(ucla.Cohort,ucla.Ethnicity,margins=True)#.plot.bar(rot=45,title='AABC')

# %%
#make a table by cohort, race
ucla=forplot.loc[forplot.Site=='UCLA'].copy()
pd.crosstab(ucla.Cohort,ucla.Race,margins=True)#.plot.bar(rot=45,title='AABC')

# %%
print("*******************")
print("**** WU ONLY *****")
wu=forplot.loc[forplot.Site=='WashU'].copy()
pd.crosstab(wu.Cohort,wu.AgeGroup5,margins=True)#.plot.bar(rot=45,title='AABC')

# %%
#make a table by cohort, ethnicity
wu=forplot.loc[forplot.Site=='WashU'].copy()
pd.crosstab(wu.Cohort,wu.Ethnicity,margins=True)#.plot.bar(rot=45,title='AABC')

# %%
#make a table by cohort, race
wu=forplot.loc[forplot.Site=='WashU'].copy()
pd.crosstab(wu.Cohort,wu.Race,margins=True)#.plot.bar(rot=45,title='AABC')

# %%
print("*******************")
print("**** UMN ONLY *****")
umn=forplot.loc[forplot.Site=='UMN'].copy()
pd.crosstab(umn.Cohort,umn.AgeGroup5,margins=True)#.plot.bar(rot=45,title='AABC')

# %%
#make a table by cohort, ethnicity
umn=forplot.loc[forplot.Site=='UMN'].copy()
pd.crosstab(umn.Cohort,umn.Ethnicity,margins=True)#.plot.bar(rot=45,title='AABC')

# %%
#make a table by cohort, race
umn=forplot.loc[forplot.Site=='UMN'].copy()
pd.crosstab(umn.Cohort,umn.Race,margins=True)#.plot.bar(rot=45,title='AABC')

# %% [markdown]
# ### Progress report format

# %%
forplot.Ethnicity.value_counts()
#forplot.columns

# %%
print("*******************")
print("**** Race x Sex in Non-Hispanics  *****")
nhisp=forplot.loc[forplot.Ethnicity=='Non-Hispanic'].copy()
pd.crosstab(nhisp.Race,nhisp.Sex,margins=True)#.plot.bar(rot=45,title='AABC')
#n=pd.crosstab(nhisp.Race,nhisp.Sex,margins=True)#.plot.bar(rot=45,title='AABC')
#n.to_csv('Non-Hispanic.csv')
#n

# %%
print("*******************")
print("**** Race x Sex in Hispanics  *****")
hisp=forplot.loc[forplot.Ethnicity=='Hispanic'].copy()
pd.crosstab(hisp.Race,hisp.Sex,margins=True)#.plot.bar(rot=45,title='AABC')
#h=pd.crosstab(hisp.Race,hisp.Sex,margins=True)#.plot.bar(rot=45,title='AABC')
#h.to_csv('Hispanic.csv')
#h

# %%
print("*******************")
print("**** Race x Sex for Unknown Ethnicities  *****")
unk=forplot.loc[forplot.Ethnicity=='Unknown'].copy()
pd.crosstab(unk.Race,unk.Sex,margins=True)#.plot.bar(rot=45,title='AABC')
#u=pd.crosstab(unk.Race,unk.Sex,margins=True)#.plot.bar(rot=45,title='AABC')
#u.to_csv('Unknown.csv')
#u

# %%
forplot.dayspassed.describe()

# %% [markdown]
# ### AABC CUMULATIVE Counts
# 

# %%
#PPP  


### create plot of AABC recruitment stats by SEX
# Create data
x=list(forplot.dayspassed) #range(1,6)
y1=list(forplot.malesum) #[1,4,6,8,9]
y2=list(forplot.femalesum)#[2,2,7,10,12]

# Basic stacked area chart.
plt.stackplot(x,y1, y2, labels=['Male:'+str(max(y1)),'Female:'+str(max(y2))])
#plt.stackplot(x,y1, y2, labels=['Male: 346','Female: 468'])
plt.legend(loc='upper left')
plt.title("AABC")
plt.xlabel('Days Passed Since 1st Recruit')
plt.ylabel('Number of Subjects');



# %%
pd.crosstab(forplot.Counterbalance,forplot.Site,margins=True)#.plot.bar(rot=45,title='AABC')

# %%
### create plot of AABC recruitment stats by SEX
# Create data
x=list(forplot.dayspassed) #range(1,6)
y1=list(forplot.CB3sum) #[1,4,6,8,9]
y2=list(forplot.CB4sum)#[2,2,7,10,12]

# Basic stacked area chart.
plt.stackplot(x,y1, y2, labels=['CB3:'+str(max(y1)),'CB4:'+str(max(y2))])
plt.title("AABC")
plt.legend(loc='upper left')
plt.xlabel('Days Passed Since 1st Recruit')
plt.ylabel('Number of Subjects');

# %%
#BY RACE
x=list(forplot.dayspassed) #range(1,6)
y1=list(forplot.whitesum) #[1,4,6,8,9]
y2=list(forplot.blacksum)#[2,2,7,10,12]
y3=list(forplot.asiansum)
y4=list(forplot.moret1sum)
#y5=list(forplot.natpacsum)
y6=list(forplot['natamersum'])
y7=list(forplot['nasum'])



# Basic stacked area chart.
plt.stackplot(x,y1,y2,y3,y4,y6,y7,labels=['White:'+str(max(y1)),'Black:'+str(max(y2)),'Asian:'+str(max(y3)),'More than one Race:'+str(max(y4)),'Nat American/Alaskan:'+str(max(y6)),'Unknown/Unreported:'+str(max(y7))])
#plt.stackplot(x,y1,y2,y3,y4,y6,y7,labels=['White: 550','Black: 171','Asian: 50','More than one Race: 22','Nat American/Alaskan: 5','Unknown/Unreported: 15'])

plt.legend(loc='upper left')
plt.title("AABC")
plt.xlabel('Days Passed Since 1st Recruit')
plt.ylabel('Number of Subjects');

# %%
#BY ETHNICITY
x=list(forplot.dayspassed) #range(1,6)
y1=list(forplot.nonhispanicsum) #[1,4,6,8,9]
y2=list(forplot.hispanicsum)#[2,2,7,10,12]
y3=list(forplot.unkhispsum)
# Basic stacked area chart.
plt.stackplot(x,y1, y2,y3, labels=['Non-Hispanic:'+str(max(y1)),'Hispanic:'+str(max(y2)),'Unknown or Not Reported:'+str(max(y3))])
#plt.stackplot(x,y1, y2,y3, labels=['Non-Hispanic: 734','Hispanic: 75','Unknown or Not Reported: 5'])
plt.legend(loc='upper left')
plt.title("AABC")
plt.xlabel('Days Passed Since 1st Recruit')
plt.ylabel('Number of Subjects');

# %%
#By Site
x=list(forplot.dayspassed) #range(1,6)
y1=list(forplot.wusum) #[1,4,6,8,9]
y2=list(forplot.umnsum)#[2,2,7,10,12]
y3=list(forplot.mghsum)
y4=list(forplot.uclasum)
# Basic stacked area chart.
plt.stackplot(x,y1, y2,y3, y4,labels=['WU:'+str(max(y1)),'UMN:'+str(max(y2)),'MGH:'+str(max(y3)),'UCLA:'+str(max(y4))])
#plt.stackplot(x,y1, y2,y3, y4,labels=['WU: 282','UMN: 198','MGH: 180','UCLA: 154'])
plt.legend(loc='upper left')
plt.title("AABC")
plt.xlabel('Days Passed Since 1st Recruit')
plt.ylabel('Number of Subjects');


# %%
#By Age Bin
x=list(forplot.dayspassed) #range(1,6)
y2=list(forplot.age30sum)#[2,2,7,10,12]
y3=list(forplot.age40sum)
y4=list(forplot.age50sum)
y5=list(forplot.age60sum)
y6=list(forplot.age70sum)
y7=list(forplot.age80sum)
y8=list(forplot.age90sum)

# Basic stacked area chart.
plt.stackplot(x,y2,y3,y4,y5,y6,y7,y8, labels=['Age [30-40):'+str(max(y2)),'Age [40-50):'+str(max(y3)),'Age [50-60):'+str(max(y4)),'Age [60-70):'+str(max(y5)),'Age [70-80):'+str(max(y6)),'Age [80-90):'+str(max(y7)),'Age [90+):'+str(max(y8))])
plt.legend(loc='upper left')
plt.title("AABC")
plt.xlabel('Days Passed Since 1st Recruit')
plt.ylabel('Number of Subjects');

# %%
#By Age Bin
x=list(forplot.dayspassed) #range(1,6)
y2=list(forplot.age1sum60)#[2,2,7,10,12]
y3=list(forplot.age2sum60)


# Basic stacked area chart.
plt.stackplot(x,y2,y3, labels=['Age < 60: '+str(max(y2)), 'Age >= 60: '+str(max(y3))])
plt.legend(loc='upper left')
plt.title("AABC")
plt.xlabel('Days Passed Since 1st Recruit')
plt.ylabel('Number of Subjects');

# %% [markdown]
# ### AABC Crosstabulations

# %%
#Crosstabs x Site

pd.crosstab(forplot.Race,forplot.Site).plot.bar(rot=45,title='AABC')
#pd.crosstab(forplot.Race,forplot.Site).to_csv('Recruitment_Stats',mode='a')
pd.crosstab(forplot.Race,forplot.Site)

# %%
pd.crosstab(forplot.Ethnicity,forplot.Site).plot.bar(rot=0,title='AABC')
#pd.crosstab(forplot.Ethnicity,forplot.Site).to_csv('Recruitment_Stats',mode='a')
pd.crosstab(forplot.Ethnicity,forplot.Site)

# %%
pd.crosstab(forplot.AgeGroup,forplot.Site).plot.bar(rot=0,title='AABC')
#pd.crosstab(forplot.AgeGroup,forplot.Site).to_csv('Recruitment_Stats',mode='a')
pd.crosstab(forplot.AgeGroup,forplot.Site)

# %%
pd.crosstab(forplot.Sex,forplot.Site).plot.bar(rot=0,title='AABC')
#pd.crosstab(forplot.Sex,forplot.Site).to_csv('Recruitment_Stats',mode='a')
pd.crosstab(forplot.Sex,forplot.Site)

# %%
pd.crosstab(forplot.AgeGroup,forplot.Race).plot.bar(rot=0,title='AABC')
#pd.crosstab(forplot.AgeGroup,forplot.Race).to_csv('Recruitment_Stats',mode='a')
pd.crosstab(forplot.AgeGroup,forplot.Race)

# %%
pd.crosstab(forplot.AgeGroup,forplot.Ethnicity).plot.bar(rot=0,title='AABC')
#pd.crosstab(forplot.AgeGroup,forplot.Ethnicity).to_csv('Recruitment_Stats',mode='a')
pd.crosstab(forplot.AgeGroup,forplot.Ethnicity)


# %%
pd.crosstab(forplot.AgeGroup,forplot.Sex).plot.bar(rot=0,title='AABC')
#pd.crosstab(forplot.AgeGroup,forplot.Sex).to_csv('Recruitment_Stats',mode='a')
pd.crosstab(forplot.AgeGroup,forplot.Sex)

# %%
#Crosstabs
pd.crosstab(forplot.Race,forplot.Sex).plot.bar(rot=45,title='AABC')
#pd.crosstab(forplot.Race,forplot.Sex).to_csv('Recruitment_Stats',mode='a')
pd.crosstab(forplot.Race,forplot.Sex)

# %%
#Crosstabs
pd.crosstab(forplot.Ethnicity,forplot.Sex).plot.bar(rot=0,title='AABC')
#pd.crosstab(forplot.Ethnicity,forplot.Sex).to_csv('Recruitment_Stats',mode='a')
pd.crosstab(forplot.Ethnicity,forplot.Sex)

# %%
#Crosstabs
pd.crosstab(forplot.Counterbalance,forplot.Sex).plot.bar(rot=0,title='AABC')
#pd.crosstab(forplot.Counterbalance,forplot.Sex).to_csv('Recruitment_Stats',mode='a')
pd.crosstab(forplot.Counterbalance,forplot.Sex)

# %%
#Crosstabs
pd.crosstab(forplot.Race,forplot.Counterbalance).plot.bar(rot=45,title='AABC')
#pd.crosstab(forplot.Race,forplot.Counterbalance).to_csv('Recruitment_Stats',mode='a')
pd.crosstab(forplot.Race,forplot.Counterbalance)

# %%
#Crosstabs
pd.crosstab(forplot.AgeGroup,forplot.Counterbalance).plot.bar(rot=0,title='AABC')
#pd.crosstab(forplot.AgeGroup,forplot.Counterbalance).to_csv('Recruitment_Stats',mode='a')
pd.crosstab(forplot.AgeGroup,forplot.Counterbalance)

# %% [markdown]
# ### New crosstabs plots including income and education group

# %%
#Crosstabs
pd.crosstab(forplot.IncomeGroup,forplot.Race).plot.bar(rot=0,title='AABC')
#pd.crosstab(forplot.AgeGroup,forplot.Counterbalance).to_csv('Recruitment_Stats',mode='a')
pd.crosstab(forplot.IncomeGroup,forplot.Race)

# %%
crosstab_result = pd.crosstab([forplot['Race']], [forplot['IncomeGroup'], forplot['EduGroup']], margins=True)
crosstab_reset = crosstab_result.reset_index()

# Using a heatmap to visualize
plt.figure(figsize=(10, 6))
sns.heatmap(crosstab_result.iloc[:-1, :-1].T, annot=True, cmap='Blues', fmt='g')

# Customize plot
plt.xticks(rotation=45)
plt.title('Crosstab Heatmap: Income and Education by Race')
plt.ylabel('Income and Education Groups')
plt.xlabel('Race')

plt.show()


