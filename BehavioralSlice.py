

## CHECK FILTER SO THAT ONLY FOLKS WHO HAVE REGISTERED FOR A VISIT GET SENT TO PRE-RELEASE FOLDERS
# NOTE THAT THIS CODE IS TAILORED TO GRAB VARIABLES FROM IN PERSON VISITS AND PHONE-BASED REGISTRATION
# NOT general ENOUGH TO GRAB Followup Variables ATM
# Output is a table and a slice dictionary

#currently supports redcap, Q-interactive, and ASA24 totals

import pandas as pd
import matplotlib.pyplot as plt
import os

### CHANGE THIS SECTION #####
##########################################################################
# INSTRUCTIONS
# Create a directory containing this program
# Create a directory within this directory called 'downloadedfiles'
# Download your AABC data (doesn't work on HCA data)... change the datadate and filenames
# ###########################################################################

# example directories end in / #
savefiles='/Users/petralenzini/work/datarequests/TestSlice/'
tmpdir='/Users/petralenzini/work/datarequests/TestSlice/downloadedfiles/'   # directory where you temporarily downloaded your data
aabcdictionary='AABC_REDCap_DataDictionary_2023-05-15.csv'                 # aabc REDCap data dictionary...necessary for automating variables at appropriate events - put in tmpdir
datadate='2023-08-28'                                                      # data at the end of any downloaded file from the PreRelease folder

#specify list of variables: TO DO: extend to other data types grab event age from inventory
# pass empty list if you don't want anything
redcapvarlist=['sex','legacy_yn','ethnic','racial','site','croms_educ','croms_income','moca_edu','bmi','bp_sitting_systolic','bp_sitting_diastolic','bp_standing_systolic','bp_standing_diastolic','hba1c','hscrp','insulin','vitamind','albumin','alkphos_total','alt_sgpt','ast_sgot','calcium','chloride','co2content','creatinine','glucose','potassium','sodium','totalbilirubin','totalprotein','ureanitrogen','friedewald_ldl','hdl','cholesterol','triglyceride','ldl','estradiol','testosterone','lh','fsh','aldosterone','dheas','cortisol','med1','med2','med3','med4','med5','med6','med7','med8','med9','med10','med11','med12','med13','med14','med15']
totalsvarlist=['KCAL','MOIS']
qvarlist=[]
# To DO: NIHTLBX_scores
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

## Leave the rest of this code alone unless you want to add in other merges ######################
# All files have 'subject' and 'redcap_event' for this purpose but ###############################

# read the aabc dictionary to grab list of registration variables
AABCdict=pd.read_csv(tmpdir+aabcdictionary,low_memory=False)
AABCdict.loc[AABCdict['Variable / Field Name'].isin(redcapvarlist)].to_csv(savefiles+'Slice_Dictionary.csv',index=False)

#separate REDCap variables into those collected at registration and those collected any other time
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

