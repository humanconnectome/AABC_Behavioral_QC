import pandas as pd
from ccf.box import LifespanBox
import re
import collections
from functions import *
from config import *
from datetime import date

#double check that config pointing to most current versions of data dictionaries

config = LoadSettings()
secret=pd.read_csv(config['config_files']['secrets'])
box = LifespanBox(cache="./tmp")

E=pd.read_csv(box.downloadFile(config['encyclopedia']),low_memory=False,encoding='ISO-8859-1')
A=pd.read_csv(box.downloadFile(config['aabc_redcap_dict']),low_memory=False,encoding='ISO-8859-1').rename(columns={'ï»¿order':'order'})[['order','Variable / Field Name', 'Form Name', 'Section Header','Field Type','Field Label', 'Choices, Calculations, OR Slider Labels','Field Annotation']]
H=pd.read_csv(box.downloadFile(config['hca_redcap_dict']),low_memory=False,encoding='ISO-8859-1')[['order','Variable / Field Name', 'Form Name', 'Section Header','Field Type', 'Field Label','Choices, Calculations, OR Slider Labels','Field Annotation']]
S=pd.read_csv(box.downloadFile(config['hca_ssaga_dict']),low_memory=False,encoding='ISO-8859-1')[['Variable / Field Name','Form Name','Section Header','Field Type','Field Label','Choices, Calculations, OR Slider Labels','Field Annotation']]

Q1=pd.read_csv(box.downloadFile(config['aabc_q_dict']),low_memory=False,encoding='ISO-8859-1')[['Variable / Field Name','Form Name','Section Header','Field Type','Field Label','Field Note','Choices, Calculations, OR Slider Labels','Field Annotation']]
Q2=pd.read_csv(box.downloadFile(config['hca_q_dict']),low_memory=False,encoding='ISO-8859-1')[['Variable / Field Name','Form Name','Section Header','Field Type','Field Label','Field Note','Choices, Calculations, OR Slider Labels','Field Annotation']]
Q=pd.concat([Q1,Q2],axis=0).drop_duplicates(subset=['Variable / Field Name'])
Q=Q.loc[~(Q['Form Name'].isin(['wais','wisc','wppsi']))]
Q.loc[Q['Field Note'].isnull()==False,'Field Label']=Q['Field Note']
Q.drop(columns=['Field Note']).to_csv('test.csv')

Echeck1=E.merge(A,on='Variable / Field Name',how='outer',indicator=True)
Echeck1=Echeck1.loc[~(Echeck1['Field Type']=='descriptive')]
Echeck1=Echeck1.loc[~(Echeck1['Form Name']=='batlq_scores')]
Echeck1.loc[Echeck1.Variable_description.isnull()==True,'Variable_description']=Echeck1['Field Label']
Echeck1=Echeck1.drop(columns=['order', 'Form Name', 'Section Header',
       'Field Type', 'Field Label']).rename(columns={'_merge':'mergeA','Choices, Calculations, OR Slider Labels':'Options','Field Annotation':'Hidden'})
Echeck1.loc[(Echeck1.mergeA.isin(['right_only','both'])) & (Echeck1['AABC Pre-Release File'].isnull()==True),'AABC Pre-Release File']='AABC_RedCap_<date>.csv'
Echeck1.to_csv('Echeck1.csv')

Echeck2=Echeck1.merge(H,on='Variable / Field Name',how='outer',indicator=True)
Echeck2=Echeck2.loc[~(Echeck2['Field Type']=='descriptive')]
Echeck2=Echeck2.loc[~(Echeck2['Form Name']=='batlq_scores')]
Echeck2.loc[Echeck2.Variable_description.isnull()==True,'Variable_description'] = Echeck2['Field Label']
Echeck2.loc[Echeck2.Hidden.isnull()==True,'Hidden'] = Echeck2['Field Annotation']
Echeck2.loc[Echeck2['Options'].isnull()==True, 'Options']=Echeck2['Choices, Calculations, OR Slider Labels']
Echeck2=Echeck2.drop(columns=['order', 'Form Name', 'Section Header',
       'Field Type', 'Field Label', 'Choices, Calculations, OR Slider Labels',
       'Field Annotation']).rename(columns={'_merge':'mergeH','HCA Pre-Release File ':'HCA Pre-Release File'})
Echeck2.loc[(Echeck2.mergeH.isin(['right_only','both'])) & (Echeck2['HCA Pre-Release File'].isnull()==True),'HCA Pre-Release File']='HCA_RedCap_<date>.csv'
Echeck2.to_csv('Echeck2.csv')

Echeck3=Echeck2.merge(S,on='Variable / Field Name',how='outer',indicator=True)
Echeck3=Echeck3.loc[~(Echeck3['Field Type']=='descriptive')]
Echeck3.loc[Echeck3.Variable_description.isnull()==True,'Variable_description'] = Echeck3['Field Label']
Echeck3.loc[Echeck3.Hidden.isnull()==True,'Hidden'] = Echeck3['Field Annotation']
Echeck3.loc[Echeck3['Options'].isnull()==True, 'Options']=Echeck3['Choices, Calculations, OR Slider Labels']
Echeck3=Echeck3.drop(columns=['Form Name', 'Section Header',
       'Field Type', 'Field Label', 'Choices, Calculations, OR Slider Labels',
       'Field Annotation',]).rename(columns={'_merge':'mergeS'})
Echeck3=Echeck3.drop(columns=['mergeA','mergeH','mergeS'])
Echeck3.to_csv('Echeck3.csv')

Echeck4=Echeck3.merge(Q,on='Variable / Field Name',how='outer',indicator=True)
Echeck4=Echeck4.loc[~(Echeck4['Field Type']=='descriptive')]
Echeck4.loc[Echeck4.Variable_description.isnull()==True,'Variable_description'] = Echeck4['Field Label']
Echeck4.loc[(Echeck4['Variable / Field Name']=='form'),'HCA Pre-Release File']=''
Echeck4=Echeck4.drop(columns=['Form Name', 'Section Header',
       'Field Type', 'Field Label', 'Choices, Calculations, OR Slider Labels',
       'Field Annotation',]).rename(columns={'_merge':'mergeQ'})
Echeck4.loc[(Echeck4.mergeQ.isin(['right_only','both'])) & (Echeck4['HCA Pre-Release File'].isnull()==True),'HCA Pre-Release File']='HCA_Q-Interactive_<date>.csv'
Echeck4.loc[(Echeck4.mergeQ.isin(['right_only','both'])) & (Echeck4['AABC Pre-Release File'].isnull()==True),'AABC Pre-Release File']='AABC_Q-Interactive_<date>.csv'
Echeck4.loc[(Echeck4.mergeQ.isin(['right_only','both'])) & (Echeck4['Form / Instrument'].isnull()==True),'Form / Instrument']='Q-INTERACTIVE RAVLT'
Echeck4.to_csv('Echeck4.csv')

#Load existing Restricted and housekeeping variables
#AABC
a=box.downloadFile(config['variablemask'])
rdrop=getlist(a,'AABC-ARMS')
rrest=getlist(a,'AABC-ARMS')
rraw=getlist(a,'TLBX-RAW')
rscore=getlist(a,'TLBX-SCORES')
restrictedQ=getlist(a,'Q')
restrictedATotals=getlist(a,'ASA24-Totals')
restrictedAResp=getlist(a,'ASA24-Resp')
restrictedATS=getlist(a,'ASA24-TS')
restrictedAINS=getlist(a,'ASA24-INS')
restrictedATNS=getlist(a,'ASA24-TNS')
restrictedAItems=getlist(a,'ASA24-Items')
rcobras=getlist(a,'COBRAS')
alldropA=rdrop+rrest+rraw+rscore+restrictedQ+restrictedQ+restrictedATotals+restrictedAResp+restrictedATS+restrictedAINS+restrictedATNS+restrictedAItems+rcobras

h=box.downloadFile(937222289846)
hQ=getlist(h,'Q')
hP=getlist(h,'PennCNP')
hR=getlist(h,'HCA')
hSSAGA=getlist(h,'SSAGA')
hraw=getlist(h,'TLBX_Raw')
hscore=getlist(h,'TLBX_Scores')
alldropH=hQ+hP+hR+hSSAGA+hraw+hscore

alldrop=alldropA+alldropH

Echeck5=Echeck4.copy()
Echeck5['Unavailable']=''
Echeck5.loc[Echeck4['Variable / Field Name'].isin(alldrop),'Unavailable']='U'

duplist=['site','redcap_event','subject','legacy_yn','PIN','age','sex']
Echeck5.loc[Echeck5['Variable / Field Name']=='hcaaabc_event','Variable / Field Name']="Harmonized visit/event shortname"
Echeck5.loc[Echeck5['Variable / Field Name']=='hcaaabc_event','Options']="V0=Registration, V1=Visit 1, V1F1=First FU after Visit1  ... VZ=Exit Interview"

#standardize some field names
Echeck5.loc[Echeck5['Variable / Field Name']=='PIN','Variable_description']='subject id concatenated with redcap_event'
Echeck5.loc[Echeck5['Variable / Field Name']=='redcap_event','Variable_description']='study-specific visit/event shortname'
Echeck5.loc[Echeck5['Variable / Field Name']=='subject','Variable_description']='Subject ID'
Echeck5.loc[Echeck5['Variable / Field Name']=='site','Variable_description']="Recruitment Site"
Echeck5.loc[Echeck5['Variable / Field Name']=='site','Options']="1=MGH, 2=UCLA, 3=UMN, 4=WU"
Echeck5.loc[Echeck5['Variable / Field Name']=='pseudo_guid','Variable_description']='NDA subjectkey'

#Beautify
Echeck5['Form / Instrument']=Echeck5['Form / Instrument'].str.title()
Echeck5['Form / Instrument']=Echeck5['Form / Instrument'].str.replace('Nih-','NIH-')
Echeck5['Form / Instrument']=Echeck5['Form / Instrument'].str.replace(' Nih',' NIH')
Echeck5['Form / Instrument']=Echeck5['Form / Instrument'].str.replace('Tlbx','TLBX')
Echeck5['Form / Instrument']=Echeck5['Form / Instrument'].str.replace('Nida','NIDA')
Echeck5['Form / Instrument']=Echeck5['Form / Instrument'].str.replace('Ssaga','SSAGA')
Echeck5['Form / Instrument']=Echeck5['Form / Instrument'].str.replace('Croms','CROMS')
Echeck5['Form / Instrument']=Echeck5['Form / Instrument'].str.replace('Moca','MOCA')
Echeck5['Form / Instrument']=Echeck5['Form / Instrument'].str.replace('Psqi','PSQI')
Echeck5['Form / Instrument']=Echeck5['Form / Instrument'].str.replace('Bat Lq2 1','BAT LQ2 1')

Echeck5['Form / Instrument']=Echeck5['Form / Instrument'].str.replace("asr","ASR")
Echeck5['Form / Instrument']=Echeck5['Form / Instrument'].str.replace("Asr","ASR")

#Echeck5['Form / Instrument']=Echeck5['Form / Instrument'].str.replace(": Asr",": ASR")
#Echeck5['Form / Instrument']=Echeck5['Form / Instrument'].str.replace(": Oasr",": OASR")
#Echeck5['Form / Instrument']=Echeck5['Form / Instrument'].str.replace("(oasr)","(OASR)")

Echeck5['Form / Instrument']=Echeck5['Form / Instrument'].str.replace("Durel","DUREL")
Echeck5['Form / Instrument']=Echeck5['Form / Instrument'].str.replace("Ces-D","CES-D")
Echeck5['Form / Instrument']=Echeck5['Form / Instrument'].str.replace("Ipip","IPIP")
Echeck5['Form / Instrument']=Echeck5['Form / Instrument'].str.replace("Gales","GALES")
Echeck5['Form / Instrument']=Echeck5['Form / Instrument'].str.replace("Phenx","PHENX")
Echeck5['Form / Instrument']=Echeck5['Form / Instrument'].str.replace("Mfq","MFQ")
Echeck5['Form / Instrument']=Echeck5['Form / Instrument'].str.replace("Vms","VMS")
Echeck5['Form / Instrument']=Echeck5['Form / Instrument'].str.replace("Straw","STRAW")
Echeck5['Form / Instrument']=Echeck5['Form / Instrument'].str.replace("Depression Ii","Depression II")
Echeck5['Form / Instrument']=Echeck5['Form / Instrument'].str.replace('Alcohol Ii','Alcohol II')
Echeck5['Form / Instrument']=Echeck5['Form / Instrument'].str.replace('Alcohol Iii','Alcohol III')
Echeck5['Form / Instrument']=Echeck5['Form / Instrument'].str.replace('Alcohol Iv','Alcohol IV')
Echeck5['Form / Instrument']=Echeck5['Form / Instrument'].str.replace('Marijuana Ii','Marijuana II')
Echeck5['Form / Instrument']=Echeck5['Form / Instrument'].str.replace('Drugs Ii','Drugs II')
Echeck5['Form / Instrument']=Echeck5['Form / Instrument'].str.replace('Drugs Iv','Drugs IV')
Echeck5['Form / Instrument']=Echeck5['Form / Instrument'].str.replace(' Ocd ',' OCD')
Echeck5['Form / Instrument']=Echeck5['Form / Instrument'].str.replace(' Ff ',' FF ')
Echeck5['Form / Instrument']=Echeck5['Form / Instrument'].str.replace(' Cat ',' CAT ')
Echeck5['Form / Instrument']=Echeck5['Form / Instrument'].str.replace('International Physical Activity Questionnaire','International Physical Activity Questionnaire (IPAQ)')

print(Echeck5.columns)

Echeck5[['Order','Form / Instrument', 'Variable / Field Name', 'Unavailable','Variable_description','Options',
       'HCA Pre-Release File', 'AABC Pre-Release File',
       'NIH Toolbox Prefix in Slice']].to_csv('Echeck5.csv')

# final tweaks by hand and uploaded as new version 10-14-2024
# REPLACE the encyclopedia fileid in the Config file so you can do this again next time.


