#DO NOT RUN THIS AS A SCRIPT.  IT IS THE HISTORY OF A SERIES OF UPLOAD PREP COMMANDS

import pandas as pd
# need to do this mostly by hand...
from functions import *
from config import *
from datetime import date

## get configuration files
outp="/Users/petralenzini/work/Behavioral/AABC/AABC_Behavioral_QC/AABC_Behavioral_QC/tmp/"
config = LoadSettings()
secret=pd.read_csv(config['config_files']['secrets'])

## get the HCA inventory for ID checking with AABC
aabcarms = redjson(tok=secret.loc[secret.source=='aabcarms','api_key'].reset_index().drop(columns='index').api_key[0])
aabcreport = redreport(tok=secret.loc[secret.source=='aabcarms','api_key'].reset_index().drop(columns='index').api_key[0],reportid='51031')

#download the inventory report from AABC for comparison
aabcinvent=getframe(struct=aabcreport,api_url=config['Redcap']['api_url10'])
aabcidvisits=idvisits(aabcinvent,keepsies=['study_id','subject_id','redcap_event_name','site'])
aabcidvisits.columns


STOP
### STOP DO NOT RUN ANYTHING BUT THE CURRENT BATCH ###
### BATCH 1 ###
batch1file="Box 8856 A-C Cruchaga.xlsx"
b1_1=pd.read_excel(outp+"batch1/"+batch1file, sheet_name='Albumin')[['ID','Results','Time Point']]
b1_1=b1_1.rename(columns={'Results':'Albumin','ID':'subject','Time Point':'redcap_event'}).copy()

for lab in['Alk Phos, Total','ALT','AST','Calcium','Chloride','CO2 Content','Cortisol','Creatinine','HDL Chol','Estradiol','FSH','LDL Chol','Glucose','HbA1c','C-React Pro','Insulin','LH','Non-HDL Chol', 'Potassium','Sodium','Testosterone','Total Bili','Total Chol','Total Protein','Triglycerides','BUN','Vit D']:
    print(lab)
    try:
        btemp=pd.read_excel(outp+"batch1/"+batch1file, sheet_name=lab)[['ID','Results','Time Point']]
        btemp = btemp.rename(columns={'Results': lab,'ID':'subject','Time Point':'redcap_event'}).copy()
        #btemp['ID'] = btemp.ID.astype(str)
        #btemp['subject'] = btemp.ID
        if lab=='HbA1c':
            hb1=btemp.copy()
        else:
            b1_1=pd.merge(b1_1,btemp,on=['subject','redcap_event'],how='outer')
    except:
        print('error with',lab)

#isolate LDL because batch 2 not specifically friedwald, and need to upload separately as info comes in.
b1_1=b1_1.rename(columns={'LDL Chol':'friedewald_ldl'})

batch1_2ndfile="Copy of Cruchaga_DHEAS_Aldosterone_06-21-23.xlsx"
b1_2=pd.read_excel(outp+"batch1/"+batch1_2ndfile, sheet_name='Cruchaga_DHEAS_Aldosterone_ (2')[['subject','Result','Test','Visit']]
b1_2=b1_2.rename(columns={'Visit':'redcap_event'}).copy()
b1_2a=b1_2.loc[(b1_2.Test.isnull()==False) & (b1_2.Test.str.contains("Aldo"))]
b1_2a=b1_2a.rename(columns={'Result':'Aldosterone'}).drop(columns=['Test'])
b1_2b=b1_2.loc[(b1_2.Test.isnull()==False) & (b1_2.Test.str.contains("DHEA"))]
b1_2b=b1_2b.rename(columns={'Result':'DHEA'}).drop(columns=['Test'])

for sheet in['Aldo','Sheet4']:
    print(sheet)
    try:
        btemp=pd.read_excel(outp+"batch1/"+batch1_2ndfile, sheet_name=sheet)[['subject','Visit','Result']]
        btemp = btemp.rename(columns={'Visit':'redcap_event','Result':'Aldosterone'}).copy()
        b1_2a=pd.concat([b1_2a,btemp])
    except:
        print('error with',sheet)

othersheet='AABC_Aldosterone results_03-03-'
otheraldo='AABC_Aldosterone results_03-03-23.xlsx'
btempo = pd.read_excel(outp + "batch1/" + otheraldo, sheet_name=othersheet, header=7)[['subject', 'Visit', 'Result']]
btempo = btempo.rename(columns={'Visit': 'redcap_event', 'Result': 'Aldosterone'}).copy()
#btempo=btempo.loc[(btempo.Aldosterone.isnull()==False) & (~(btempo.Aldosterone.str.contains('repeat')))]
b1_2a = pd.concat([b1_2a, btempo])

btempd=pd.read_excel(outp+"batch1/"+batch1_2ndfile, sheet_name='DHEA')[['subject','Visit','Result']]
btempd = btempd.rename(columns={'Visit':'redcap_event','Result':'DHEA'}).copy()
b1_2b=pd.concat([b1_2b,btempd])

batch1_3rdfile="Box 8856 HbA1c-Cruchaga.xlsx"
sheet="Box 8856 HbA1c-Cruchaga ID fix"
btemph=pd.read_excel(outp+"batch1/"+batch1_3rdfile, sheet_name=sheet)[['Combo','Time Point','Results']]
btemph = btemph.rename(columns={'Time Point':'redcap_event','Combo':'subject','Results':'HbA1c'}).copy()


### STOP DO NOT RUN ANYTHING BUT THE CURRENT BATCH ###
### BATCH 2 ###

batch2file="Copy of Box 8910 A-B E-F Cruchaga.xlsx"
sheet="Box 8910 A-B, E-F Cruchaga"
btemp2=pd.read_excel(outp+"batch2/"+batch2file, sheet_name=sheet,header=8)[['subject','Time Point','Test','Result']]
btemp2=btemp2.rename(columns={'Time Point':'redcap_event'})

b2 = btemp2.loc[btemp2.Test == 'Albumin']
b2=b2.rename(columns={'Result': 'Albumin'})
b2=b2.drop(columns=['Test'])

for i in list(btemp2.Test.unique()):
    print(i)
    if i=='Albumin':
        pass
    elif i=='HbA1c':
        hb2 = btemp2.loc[btemp2.Test == i]
        hb2 = hb2.rename(columns={'Result': i})
        hb2 = hb2.drop(columns=['Test'])
    else:
        try:
            part = btemp2.loc[btemp2.Test==i]
            part=part.rename(columns={'Result':i})
            part=part.drop(columns=['Test'])
            b2=pd.merge(b2,part,on=['subject','redcap_event'],how='outer')
        except:
            print('error with',lab)
b2=b2.rename(columns={'Total Proteins':'Total Protein'})

#All together now
aabcidvisits=aabcidvisits.loc[aabcidvisits.redcap_event.isin(['V1','V2','V3','V4'])][['study_id','subject','redcap_event','redcap_event_name','site']]
a=pd.concat([b1_1,b2]).drop_duplicates()
c=a.merge(b1_2a, on=['subject','redcap_event'],how='outer')
d=c.merge(b1_2b, on=['subject','redcap_event'],how='outer')
hba=pd.concat([hb1,hb2,btemph]).drop_duplicates()
e=d.merge(hba, on=['subject','redcap_event'],how='outer')
f=e.merge(aabcidvisits,on=['subject','redcap_event'],how='right').drop_duplicates()

#rename columns for import:
cruchaga=['Albumin','Alk Phos, Total','ALT','AST','Calcium','Chloride','CO2 Content','Cortisol','Creatinine','HDL Chol','Estradiol','FSH','Glucose','C-React Pro','Insulin','LH','Potassium','Sodium','Testosterone','Total Bili','Total Chol','Triglycerides','BUN','Vit D','Total Protein','Aldosterone','DHEA','HbA1c']
redcap=['albumin','alkphos_total','alt_sgpt','ast_sgot','calcium','chloride','co2content','cortisol','creatinine','hdl','estradiol','fsh','glucose','hscrp','insulin','lh','potassium','sodium','testosterone','totalbilirubin','cholesterol','triglyceride','ureanitrogen','vitamind','totalprotein','aldosterone','dheas','hba1c']
renames=dict(zip(cruchaga,redcap))
f=f.rename(columns=renames)

g=f[['study_id','redcap_event_name','site','subject']+redcap]
g.to_csv('Labs_Uploaded_2RedCap_'+date.today().strftime("%Y-%m-%d")+'.csv',index=False)
f[['study_id','redcap_event_name','friedewald_ldl']].to_csv('FriedLDL_Uploaded_2RedCap_'+date.today().strftime("%Y-%m-%d")+'.csv',index=False)
f[['study_id','redcap_event_name','LDL Chol']].rename(columns={'LDL Chol': 'friedewald_ldl'}).to_csv('FriedLDL_2batchUploaded_2RedCap_'+date.today().strftime("%Y-%m-%d")+'.csv',index=False)

#another batch2
batch2file="Box 8910 E-F.2-Cruchaga.xlsx"
sheet="Box 8910 E-F.2-Cruchaga"
btemp2=pd.read_excel(outp+"batch2/"+batch2file, sheet_name=sheet,header=8)
btemp2['subject']="HCA"+btemp2['Tube ID'].str.split('-',expand=True)[0].str.strip()
btemp2['redcap_event'] = btemp2['Time Point'].str.replace('.', "").str.strip()
btemp2=btemp2[['subject','redcap_event','Test','Result']]

b2=pd.DataFrame(columns=['subject','redcap_event'])
for i in list(btemp2.Test.unique()):
    print(i)
    try:
        part = btemp2.loc[btemp2.Test==i]
        part = part.rename(columns={'Result':i})
        part = part.drop(columns=['Test'])
        b2 = pd.merge(b2, part, on=['subject', 'redcap_event'], how='outer')
    except:
        print('error with',lab)
b2=b2.rename(columns={'Total Proteins':'Total Protein'})
#cruchaga=['Albumin','Alk Phos, Total','ALT (SGPT)', 'AST (SGOT)', 'Calcium', 'Chloride', 'CO2 Content', 'Cortisol', 'Creatinine', 'Direct HDL Cholesterol', 'Estradiol, e601', 'Follicle-Stimulating Hormone', 'Friedewald LDL Chol', 'Glucose', 'HS C-Reactive Prot', 'Insulin', 'Luteinizing Hormone', 'Potassium', 'Sodium', 'Testosterone', 'Total Bilirubin', 'Total Cholesterol', 'Total Protein', 'Triglycerides', 'Urea Nitrogen', 'Vitamin D']
cruchaga=['Albumin','Alk Phos, Total','ALT (SGPT)','AST (SGOT)','Calcium','Chloride','CO2 Content','Cortisol','Creatinine','Direct HDL Cholesterol','Estradiol, e601','Follicle-Stimulating Hormone','Friedewald LDL Chol','Glucose','HS C-Reactive Prot','Insulin','Luteinizing Hormone','Potassium','Sodium','Testosterone','Total Bilirubin','Total Cholesterol','Total Protein','Triglycerides','Urea Nitrogen','Vitamin D']
redcap=['albumin',  'alkphos_total',  'alt_sgpt',  'ast_sgot',  'calcium', 'chloride',  'co2content', 'cortisol', 'creatinine',   'hdl',             'estradiol',        'fsh',                         'friedewald_ldl',  'glucose','hscrp',             'insulin',    'lh',             'potassium','sodium','testosterone','totalbilirubin','cholesterol',        'totalprotein', 'triglyceride',   'ureanitrogen',  'vitamind']

renames=dict(zip(cruchaga,redcap))
f=b2.rename(columns=renames)
f=f.merge(aabcidvisits,on=['subject','redcap_event'],how='inner').drop_duplicates()

g=f[['study_id','redcap_event_name','subject','redcap_event']+redcap]
g.to_csv('Labs_Uploaded_2RedCap_'+date.today().strftime("%Y-%m-%d")+'.csv',index=False)



### STOP DO NOT RUN ANYTHING BUT THE CURRENT BATCH ###
### BATCH 3 ###
batch3file="Batch 3 Cruchaga Results 110723.xlsx"
sheet="Box 8939 A-C Cruchaga"
btemp2=pd.read_excel(outp+"batch3/"+batch3file, sheet_name=sheet)[['Combined ID','Test','Result']]
btemp2=btemp2.rename(columns={'Combined ID':'PIN'})
btemp2=btemp2.loc[btemp2.PIN.isnull()==False]

b2=pd.DataFrame(columns=['PIN'])
for i in list(btemp2.Test.unique()):
    try:
        part = btemp2.loc[btemp2.Test==i]
        part=part.rename(columns={'Result':i})
        part=part.drop(columns=['Test'])
        b2=pd.merge(b2,part,on=['PIN'],how='outer')
    except:
        print('error with',i)
redcols=['PIN','dheas','hba1c','albumin','alkphos_total','alt_sgpt','ast_sgot','calcium','chloride','co2content','creatinine','hdl','friedewald_ldl','glucose','hscrp','nonhdl','potassium','sodium','totalbilirubin','cholesterol','totalprotein','triglyceride','ureanitrogen','cortisol','estradiol','fsh','insulin','lh','testosterone','vitamind']
b2.columns=redcols
aabcidvisits['PIN']=aabcidvisits.subject+'_'+aabcidvisits.redcap_event
b2.shape
b3=b2.merge(aabcidvisits,on='PIN',how='left')
dropm=['PIN','subject','site','subject_id','redcap_event']
b3.drop(columns=dropm).to_csv('Batch3_upload_4Dec2023.csv',index=False)

### STOP DO NOT RUN ANYTHING BUT THE CURRENT BATCH ###
### BATCH 4 ###
batch4file1="02062024 Boxes 8939 B  8985 A - Cruchaga- DHEAS.xlsx"
sheet="Modified"
btemp4f1=pd.read_excel(outp+"batch4/"+batch4file1, sheet_name=sheet)[['Combination','Test','Result']]
btemp4f1=btemp4f1.rename(columns={'Combination':'PIN'})
btemp4f1=btemp4f1.loc[btemp4f1.PIN.isnull()==False]
btemp4f1_1=btemp4f1.drop(columns='Test').rename(columns={'Result':'DHEA'}).copy()
#typos
s=['HCA7700265_V2','HCA8712883_V2']
btemp4f1_1.loc[btemp4f1_1.PIN.isin(s),'PIN']=btemp4f1_1.PIN.str.replace('V2','V3')


#need to loop through the sheets in this file because need to grab timepoint
batch4file2="USE THIS Copy of Boxes 8985 C-G -Cruchaga.xlsx"
b1_1=pd.read_excel(outp+"batch4/"+batch4file2, sheet_name='Albumin')[['TimePoint +ID','Result']]
b1_1=b1_1.rename(columns={'Result':'Albumin','ID':'subject','TimePoint +ID':'PIN'}).copy()

for lab in['Alk Phos','ALT','AST','Calcium','Chloride','CO2','Cortisol','Creatinine','HDL','Estradiol','FSH','LDL','Glucose','HbA1c','C-Reactive','Insulin','LH','Potassium','Sodium','Testosterone','Bili','TotalCHL','Protein','Triglycerides','BUN','Vit D']:
    print(lab)
    try:
        btemp=pd.read_excel(outp+"batch4/"+batch4file2, sheet_name=lab)[['TimePoint +ID','Result']]
        btemp = btemp.rename(columns={'Result': lab,'TimePoint +ID':'PIN'}).copy()
        b1_1=pd.merge(b1_1,btemp,on=['PIN'],how='outer')
    except:
        print('error with',lab)
btemp4f1.to_csv(outp+"batch4/"+'test.csv')
batch4=pd.merge(b1_1,btemp4f1_1,on="PIN",how='outer')
batch4['subject']=batch4.PIN.str.split('_',expand=True)[0]
batch4['redcap_event']=batch4.PIN.str.split('_',expand=True)[1]

batch4b=pd.merge(aabcidvisits,batch4,on=['subject','redcap_event'],how='right')#.drop(columns=['subject','site','subject_id','redcap_event','PIN'])

cruchaga=['Albumin', 'Alk Phos', 'ALT', 'AST',
       'Calcium', 'Chloride', 'CO2', 'Cortisol', 'Creatinine', 'HDL',
       'Estradiol', 'FSH', 'LDL', 'Glucose', 'C-Reactive', 'Insulin', 'LH',
        'Potassium', 'Sodium', 'Testosterone', 'TotalCHL', 'Protein',
       'Triglycerides', 'BUN', 'Vit D', 'DHEA','HbA1c','Bili']

redcap=['albumin',  'alkphos_total',  'alt_sgpt',  'ast_sgot',
        'calcium', 'chloride',  'co2content', 'cortisol', 'creatinine',   'hdl',
        'estradiol',   'fsh', 'friedewald_ldl', 'glucose','hscrp', 'insulin', 'lh',
         'potassium','sodium','testosterone','cholesterol',  'totalprotein',
        'triglyceride',   'ureanitrogen',  'vitamind','dheas','hba1c','totalbilirubin']

renames=dict(zip(cruchaga,redcap))
f=batch4b.rename(columns=renames)


f.drop(columns=['subject','redcap_event','PIN','site']).to_csv(outp+"batch4/"+'Batch4_forUpload_v2.csv',index=False)

#one more file for Batch 4 - received March 6, 2024
batch4file3="Boxes 8995 A-B Cruchaga2.xlsx"
sheet="Modified"
btemp4f3=pd.read_excel(outp+"batch4/"+batch4file3, sheet_name=sheet)[['Combined','Test','Result']]
btemp4f3=btemp4f3.rename(columns={'Combined':'PIN'})
btemp4f3=btemp4f3.loc[btemp4f3.PIN.isnull()==False]
b43=pd.DataFrame(columns={'PIN'})
for i in list(btemp4f3.Test.unique()):
    try:
        part = btemp4f3.loc[btemp4f3.Test==i]
        part=part.rename(columns={'Result':i})
        part=part.drop(columns=['Test'])
        b43=pd.merge(b43,part,on=['PIN'],how='outer')
    except:
        print('error with',i)

cruchaga=['HbA1c', 'Albumin', 'Alk Phos, Total', 'ALT (SGPT)', 'AST (SGOT)', 'Calcium', 'Chloride', 'CO2 Content', 'Creatinine', 'Direct HDL Cholesterol', 'Friedewald LDL Chol', 'Glucose', 'HS C-Reactive Prot', 'Non-HDL Cholesterol', 'Potassium', 'Sodium',  'Total Bilirubin', 'Total Cholesterol', 'Total Protein', 'Triglycerides', 'Urea Nitrogen',  'Cortisol', 'Estradiol, e601', 'Follicle-Stimulating Hormone', 'Insulin', 'Luteinizing Hormone', 'Testosterone', 'Vitamin D']
redcap= ['hba1c',  'albumin',  'alkphos_total',  'alt_sgpt',  'ast_sgot',   'calcium', 'chloride',  'co2content', 'creatinine',     'hdl',                  'friedewald_ldl',        'glucose',              'hscrp', 'non-hdl',           'potassium','sodium',  'totalbilirubin',     'cholesterol', 'totalprotein', 'triglyceride',     'ureanitrogen',    'cortisol', 'estradiol',      'fsh',                           'insulin', 'lh',                 'testosterone',    'vitamind'  ]

renames=dict(zip(cruchaga,redcap))
ff=b43.rename(columns=renames)

aabcidvisits['PIN']=aabcidvisits['subject']+"_"+aabcidvisits['redcap_event']
batch4_3=pd.merge(aabcidvisits[['PIN','redcap_event_name','study_id']],ff,on=['PIN'],how='right').drop(columns=['PIN'])
batch4_3.to_csv(outp+"batch4/"+'Batch4b_forUpload.csv',index=False)

#batch 5 uploaded by Preston

#batch6
folder="/Users/petralenzini/work/Behavioral/AABC/AABC_Behavioral_QC/AABC_Behavioral_QC/tmp/tmp2/batch6/"
fileb="Box 9057 Box A,C,D,E,F Cruchaga.xls"
sheet="Modified"
btemp2=pd.read_excel(folder+fileb, sheet_name=sheet)[['TimePoint+ID','Test','Results']]
btemp2=btemp2.rename(columns={'TimePoint+ID':'PIN'})
btemp2=btemp2.loc[btemp2.PIN.isnull()==False]
b6=pd.DataFrame(columns={'PIN'})
for i in list(btemp2.Test.unique()):
    try:
        part = btemp2.loc[btemp2.Test==i]
        part=part.rename(columns={'Results':i})
        part=part.drop(columns=['Test'])
        b6=pd.merge(b6,part,on=['PIN'],how='outer')
    except:
        print('error with',i)
cruchaga=['DHEA-S', 'Albumin', 'Alk Phos, Total', 'ALT (SGPT)',
       'AST (SGOT)', 'Calcium', 'Chloride', 'CO2 Content', 'Creatinine',
       'Direct HDL Cholesterol', 'Friedewald LDL Chol', 'Glucose',
       'HS C-Reactive Prot', 'Non-HDL Cholesterol', 'Potassium', 'Sodium',
       'Total Bilirubin', 'Total Cholesterol', 'Total Protein',
       'Triglycerides', 'Urea Nitrogen', 'Cortisol', 'Estradiol, e601',
       'Follicle-Stimulating Hormone', 'Insulin', 'Luteinizing Hormone',
       'Testosterone', 'Vitamin D', 'HbA1c']
redcap= ['dheas','albumin',  'alkphos_total',  'alt_sgpt',
         'ast_sgot',   'calcium', 'chloride',  'co2content', 'creatinine',
         'hdl',                  'friedewald_ldl',        'glucose',
         'hscrp', 'non-hdl',           'potassium','sodium',
         'totalbilirubin',     'cholesterol', 'totalprotein',
         'triglyceride',     'ureanitrogen',    'cortisol', 'estradiol',
         'fsh',                           'insulin', 'lh',
         'testosterone',    'vitamind', 'hba1c']
##typos
#s=['HCA7700265_V2','HCA8712883_V2']
#btemp4f1_1.loc[btemp4f1_1.PIN.isin(s),'PIN']=btemp4f1_1.PIN.str.replace('V2','V3')
renames=dict(zip(cruchaga,redcap))
ff=b6.rename(columns=renames)
#lab confirmed nightmare typo
ff.loc[ff.PIN=='HCA6670479_V2','PIN']='HCA6670479_V3'

aabcidvisits['PIN']=aabcidvisits['subject']+"_"+aabcidvisits['redcap_event']
batch6=pd.merge(aabcidvisits[['PIN','redcap_event_name','study_id']],ff,on=['PIN'],how='right')#.drop(columns=['PIN'])
batch6.to_csv(outp+"tmp2/batch6/"+'Batch6_forUpload.csv',index=False)

# batch 6 b
# HCA8379495_V3
#batch6
folder="/Users/petralenzini/work/Behavioral/AABC/AABC_Behavioral_QC/AABC_Behavioral_QC/tmp/tmp2/batch6/"
fileb="HCA8379495.xlsx"
sheet="Sheet1"
btemp2=pd.read_excel(folder+fileb, sheet_name=sheet)[['Time Point','ID','Test','Results']]
btemp2['PIN']=btemp2['ID'].str.strip()+'_'+btemp2['Time Point'].str.strip()
b6=pd.DataFrame(columns={'PIN'})
for i in list(btemp2.Test.unique()):
    try:
        part = btemp2.loc[btemp2.Test==i]
        part=part.rename(columns={'Results':i})
        part=part.drop(columns=['Test','Time Point','ID'])
        b6=pd.merge(b6,part,on=['PIN'],how='outer')
    except:
        print('error with',i)

cruchaga=['Cortisol', 'Estradiol, e601', 'Follicle-Stimulating Hormone',
       'Insulin', 'Luteinizing Hormone', 'Testosterone', 'Vitamin D', 'HbA1c',
       'Albumin', 'Alk Phos, Total', 'ALT (SGPT)', 'AST (SGOT)', 'Calcium',
       'Chloride', 'CO2 Content', 'Creatinine', 'Direct HDL Cholesterol',
       'Friedewald LDL Chol', 'Glucose', 'HS C-Reactive Prot',
       'Non-HDL Cholesterol', 'Potassium', 'Sodium', 'Total Bilirubin',
       'Total Cholesterol', 'Total Protein', 'Triglycerides', 'Urea Nitrogen']

redcap= ['cortisol','estradiol','fsh',
         'insulin', 'lh','testosterone',    'vitamind', 'hba1c',
         'albumin' ,'alkphos_total',  'alt_sgpt','ast_sgot',   'calcium',
         'chloride',  'co2content', 'creatinine','hdl',
         'friedewald_ldl', 'glucose', 'hscrp',
         'non-hdl',  'potassium','sodium','totalbilirubin',
         'cholesterol', 'totalprotein',  'triglyceride',     'ureanitrogen']

##typos
#s=['HCA7700265_V2','HCA8712883_V2']
#btemp4f1_1.loc[btemp4f1_1.PIN.isin(s),'PIN']=btemp4f1_1.PIN.str.replace('V2','V3')
renames=dict(zip(cruchaga,redcap))
ff=b6.rename(columns=renames)

aabcidvisits['PIN']=aabcidvisits['subject']+"_"+aabcidvisits['redcap_event']
batch6b=pd.merge(aabcidvisits[['PIN','redcap_event_name','study_id']],ff,on=['PIN'],how='right')#.drop(columns=['PIN'])
batch6b.to_csv(outp+"tmp2/batch6/"+'Batch6b_forUpload.csv',index=False)


#sheet="Modified"
#btemp4f2=pd.read_excel(outp+"batch4/"+batch4file2, sheet_name=sheet)[['Combo','Test','Result']]
#btemp4f2=btemp4f2.rename(columns={'Combo':'PIN'})
#btemp4f2=btemp4f2.loc[btemp4f2.PIN.isnull()==False]


##########
#Batch 7
folder="/Users/petralenzini/work/Behavioral/AABC/AABC_Behavioral_QC/AABC_Behavioral_QC/tmp/tmp2/batch7/"
fileb="Box 9120 A-B, E-K Cruchaga_Corrected.xlsx"
sheet="Modified"
btemp2=pd.read_excel(folder+fileb, sheet_name=sheet)[['TimePoint+ID','Test','Results']]
btemp2['PIN']=btemp2['TimePoint+ID'].str.strip()

#btemp2['PIN']=btemp2['ID'].str.strip()+'_'+btemp2['Time Point'].str.strip()
b7=pd.DataFrame(columns={'PIN'})
for i in list(btemp2.Test.unique()):
    try:
        part = btemp2.loc[btemp2.Test==i]
        part=part.rename(columns={'Results':i})
        part=part.drop(columns=['Test','TimePoint+ID'])
        b7=pd.merge(b7,part,on=['PIN'],how='outer')
    except:
        print('error with',i)

cruchaga=['DHEA-S', 'Albumin', 'Alk Phos, Total', 'ALT (SGPT)',
       'AST (SGOT)', 'Calcium', 'Chloride', 'CO2 Content', 'Creatinine',
       'Direct HDL Cholesterol', 'Friedewald LDL Chol', 'Glucose',
       'HS C-Reactive Prot', 'Non-HDL Cholesterol', 'Potassium', 'Sodium',
       'Total Bilirubin', 'Total Cholesterol', 'Total Protein',
       'Triglycerides', 'Urea Nitrogen', 'Cortisol', 'Estradiol, e601',
       'Follicle-Stimulating Hormone', 'Insulin', 'Luteinizing Hormone',
       'Testosterone', 'Vitamin D', 'HbA1c']

redcap= ['dheas','albumin' ,'alkphos_total',  'alt_sgpt',
         'ast_sgot',   'calcium','chloride',  'co2content', 'creatinine',
         'hdl','friedewald_ldl', 'glucose',
         'hscrp','non-hdl',  'potassium','sodium',
         'totalbilirubin',  'cholesterol', 'totalprotein',
         'triglyceride',     'ureanitrogen','cortisol','estradiol',
         'fsh', 'insulin', 'lh',
         'testosterone',    'vitamind', 'hba1c']

##typos
#s=['HCA7700265_V2','HCA8712883_V2']
#btemp4f1_1.loc[btemp4f1_1.PIN.isin(s),'PIN']=btemp4f1_1.PIN.str.replace('V2','V3')
renames=dict(zip(cruchaga,redcap))
ff=b7.rename(columns=renames)

aabcidvisits['PIN']=aabcidvisits['subject']+"_"+aabcidvisits['redcap_event']
batch7=pd.merge(aabcidvisits[['PIN','redcap_event_name','study_id']],ff,on=['PIN'],how='right')#.drop(columns=['PIN'])
#batch7=pd.merge(aabcidvisits[['PIN','redcap_event_name','study_id']],ff,on=['PIN'],how='outer',indicator=True)#.drop(columns=['PIN'])
reordercol=['study_id','redcap_event_name']+[a for a in batch7.columns if a !='redcap_event_name' and a !='study_id']
batch7[reordercol].drop(columns=['PIN','non-hdl']).to_csv(outp+"tmp2/batch7/"+'Batch7_forUpload.csv',index=False)

##########