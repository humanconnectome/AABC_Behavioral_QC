import os
import pandas as pd
import datetime

issuesdir="/Users/petralenzini/work/Behavioral/AABC/AABC_Behavioral_QC/AABC_Behavioral_QC/Issues/"
issuelist=os.listdir(issuesdir)
Allissues=pd.DataFrame()
for i in issuelist:
    print(i)
    if "All_Issues" in i:
        try:
            iss=pd.read_csv(issuesdir+i)
            Allissues=pd.concat([Allissues,iss],axis=0)
        except:
            print(i,'will not load')


Allissues=Allissues.loc[Allissues.subject.isnull()==False]
Allissues=Allissues.loc[~(Allissues.subject=='')]

Allissues=Allissues.loc[Allissues.datatype.isnull()==False].copy()
Allissues=Allissues.loc[~(Allissues.datatype=='')]

#Allissues.loc[Allissues.issue_age.isnull()==True,'issue_age']='0 days'
Allissues['days']=Allissues.issue_age.str.split(expand=True)[0]#.astype('int')
Allissues.loc[Allissues.days.isnull(),'days']=0
Allissues['days']=Allissues.days.astype('int')
#filteredAll=Allissues.loc[((Allissues.code=='PINK') & (Allissues.issue_age.dt.days>7)) | ((Allissues.code=='RED') & (Allissues.issue_age.dt.days>4)) | ((Allissues.code=='RED') & (Allissues.issue_age.dt.days.isnull()==True)) |  ((Allissues.code=='ORANGE') & (Allissues.issue_age.dt.days>18)) |  ((Allissues.code=='YELLOW') & (Allissues.issue_age.dt.days>28)) |  ((Allissues.code=='GREEN') & (Allissues.issue_age.dt.days>35)) ]
filteredAll=Allissues.loc[((Allissues.code=='PINK') & (Allissues.days>7)) | ((Allissues.code=='RED') & (Allissues.days>4)) | ((Allissues.code=='RED') & (Allissues.days.isnull()==True)) |  ((Allissues.code=='ORANGE') & (Allissues.days>18)) |  ((Allissues.code=='YELLOW') & (Allissues.days>28)) |  ((Allissues.code=='GREEN') & (Allissues.days>35)) ]


filteredAll=filteredAll.drop_duplicates(subset=['subject', 'redcap_event','datatype'])
filteredAll.to_csv('allissuestest.csv',index=False)

print("Issues by site:",filteredAll.site.value_counts(dropna=False))
print("Issues by datatype:",filteredAll.datatype.value_counts(dropna=False))
print("Issues by reason:",filteredAll.reason.value_counts(dropna=False))
filteredAll.reason.value_counts(dropna=False).to_csv('reasons539.csv')
print("Open Issues by site")
pd.DataFrame(filteredAll.reason.value_counts()).to_csv('testreasons2.csv')
#pd.crosstab(filteredAll.reason,filteredAll.datatype,margins=True).to_csv('crosstabreasons.csv')