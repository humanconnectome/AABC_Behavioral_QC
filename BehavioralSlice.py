# NOTE THAT THIS CODE IS TAILORED TO GRAB VARIABLES FROM IN PERSON VISITS AND PHONE-BASED REGISTRATION
# NOT SMART ENOUGH TO GRAB Followup Variables ATM
# Output is a table and a slice dictionary


import pandas as pd
import matplotlib.pyplot as plt
import os

### CHANGE THIS SECTION #####
##########################################################################
tmpdir='/Users/petralenzini/work/datarequests/Lavretsky/downloadedfiles/'  # directory where you downloaded your data
savefiles='/Users/petralenzini/work/datarequests/Lavretsky/'
aabcdictionary='AABC_REDCap_DataDictionary_2023-05-15.csv'                 # aabc REDCap data dictionary...necessary for automating variables at appropriate events
hcadatadictionary='HCA_RedCap_DataDictionary_2022-01-28.csv'               # for comparing HCA variables to AABC variables
datadate='2023-08-28'                                                      # data at the end of any downloaded file from the PreRelease folder

#specify list of variables: TO DO: extend to other data types
inventorylist=['event_age']
redcapvarlist=['sex','legacy_yn','ethnic','racial','site','croms_educ','croms_income','moca_edu','bmi','bp_sitting_systolic','bp_sitting_diastolic','bp_standing_systolic','bp_standing_diastolic','hba1c','hscrp','insulin','vitamind','albumin','alkphos_total','alt_sgpt','ast_sgot','calcium','chloride','co2content','creatinine','glucose','potassium','sodium','totalbilirubin','totalprotein','ureanitrogen','friedewald_ldl','hdl','cholesterol','triglyceride','ldl','estradiol','testosterone','lh','fsh','aldosterone','dheas','cortisol','med1','med2','med3','med4','med5','med6','med7','med8','med9','med10','med11','med12','med13','med14','med15']
totalsvarlist=['KCAL','MOIS','PROT','TFAT','CARB','ALC','CAFF','THEO','SUGR','FIBE','CALC','IRON','MAGN','PHOS','POTA','SODI','ZINC','COPP','SELE','VC','VB1','VB2','NIAC','VB6','FOLA','FA','FF','FDFE','VB12','VARA','RET','BCAR','ACAR','CRYP','LYCO','LZ','ATOC','VK','CHOLE','SFAT','S040','S060','S080','S100','S120','S140','S160','S180','MFAT','M161','M181','M201','M221','PFAT','P182','P183','P184','P204','P205','P225','P226','VITD','CHOLN','VITE_ADD','B12_ADD','F_TOTAL','V_TOTAL','V_REDOR_TOTAL','V_STARCHY_TOTAL','G_TOTAL','PF_TOTAL','PF_MPS_TOTAL','D_TOTAL','OILS','SOLID_FATS','ADD_SUGARS','A_DRINKS']
qvarlist=['q_unusable','unusable_specify','ravlt_two','ravlt_pea_ravlt_sd_tc','ravlt_delay_scaled','ravlt_delay_completion','ravlt_discontinue','ravlt_reverse','ravlt_pea_ravlt_sd_trial_i_tc','ravlt_pea_ravlt_sd_trial_ii_tc','ravlt_pea_ravlt_sd_trial_iii_tc','ravlt_pea_ravlt_sd_trial_iv_tc','ravlt_pea_ravlt_sd_trial_v_tc','ravlt_pea_ravlt_sd_listb_tc','ravlt_pea_ravlt_sd_trial_vi_tc','ravlt_recall_correct_trial1','ravlt_recall_correct_trial2','ravlt_recall_correct_trial3','ravlt_recall_correct_trial4','ravlt_delay_recall_correct','ravlt_delay_recall_intrusion','ravlt_delay_total_intrusion','ravlt_delay_total_repetitions']
# NIHTLBX_scores
# Cobra
# Inventory

## other lists requested
#baselist=['age','sex','ethnic','racial','site','croms_racial','croms_ethnic']
#gales=list(AABCdict.loc[AABCdict['Variable / Field Name'].str.contains('gales')]['Variable / Field Name'])
#durel=list(AABCdict.loc[AABCdict['Variable / Field Name'].str.contains('durel')]['Variable / Field Name'])
#formlist=['perceived_everyday_discrimination_block3','neighborhood_disorderneighborhood_social_cohesion','ongoing_chronic_stressors_scale_block3','barriers_to_healthcare_checklist_block3','access_to_healthcare_phenx_block3']
#others=list(AABCdict.loc[AABCdict['Form Name'].isin(formlist)]['Variable / Field Name'])
#red=baselist+gales+durel+others
#redcapvarlist=[i for i in red if "miss" not in i]

#Do you want distributions and or a merged data dictionary?
wantplots=True  # or False
skip_plots=['subject','redcap_event']

#this part not complete.  ATM just merges HCA and AABC REDCap data dictionaries.  Ulitmately want a slice dictionary.
# for now, just save the REDCap slice
AABCdict.loc[AABCdict['Variable / Field Name'].isin(redcapvarlist)].to_csv(savefiles+'Slice_Dictionary.csv',index=False)
wantdicts=False

####################################################################################################


## Leave this section alone #####################################################
# read the aabc dictionary to grab list of registration variables
AABCdict=pd.read_csv(tmpdir+aabcdictionary,low_memory=False)

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

# merge REDCap parts - there will be more observations in registration than in visit, due to delay between the two
# only one registration per person.  Eventually will be multiple visits
REDCap=pd.merge(REDCapReg.loc[~(REDCapReg.subject=='')],REDCapVisit,on='subject',how='left')

Alldata_a=pd.merge(REDCap,Totals,on=['subject','redcap_event'],how='left')
Alldata=pd.merge(Alldata_a,Qdata,on=['subject','redcap_event'],how='left')
#reorder
rightcols=[col for col in Alldata.columns if (col != "redcap_event" and col !="subject")]
#create csv
Alldata[['subject','redcap_event']+rightcols].to_csv(savefiles+"AABC_Behavioral_Data_Slice_"+datadate+'.csv',index=False)

# super basic plots of variable distributions
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

# now merge data dictionaries
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

