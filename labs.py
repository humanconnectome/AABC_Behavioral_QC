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



#BATCH 2
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

