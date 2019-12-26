#! /usr/bin/python3
# Travelshed Mapping
# OpenTripPlanner Guide: http://docs.opentripplanner.org/en/latest/
# OpenTripPlanner API Doc: http://dev.opentripplanner.org/apidoc/1.0.0/index.html

import datetime
import requests
import geopandas as gpd
import pandas as pd
import numpy as np
import multiprocessing as mp
import os

start=datetime.datetime.now()

pd.set_option('display.max_columns', None)
path='/home/mayijun/TRAVELSHED/'
#path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2018/TRAVELSHEDREVAMP/'
#path='E:/TRAVELSHED/'



# Summarize travelshed outputs
# NYC Res Censust Tracts
resbk=pd.DataFrame()
for i in os.listdir(path+'nyctract/res/'):
    tp=pd.read_csv(path+'nyctract/res/'+i,dtype=str)
    tp=tp.set_index('blockid')
    resbk=pd.concat([resbk,tp],axis=1)
resbk.to_csv(path+'nyctract/resbk.csv',index=True)
resct=pd.read_csv(path+'nyctract/resbk.csv',dtype=str)
resct=resct.set_index('blockid')
resloclist=sorted(resct.columns)
for i in resct.columns:
    resct[i]=pd.to_numeric(resct[i])
resct=resct.replace(999,np.nan)
resct['tractid']=[str(x)[0:11] for x in resct.index]
resct=resct.groupby(['tractid'])[resloclist].median(skipna=True)
resct.to_csv(path+'nyctract/resct.csv',index=True,na_rep='999')

# NYC Work Censust Tracts
workbk=pd.DataFrame()
for i in os.listdir(path+'nyctract/work/'):
    tp=pd.read_csv(path+'nyctract/work/'+i,dtype=str)
    tp=tp.set_index('blockid')
    workbk=pd.concat([workbk,tp],axis=1)
workbk.to_csv(path+'nyctract/workbk.csv',index=True)
workct=pd.read_csv(path+'nyctract/workbk.csv',dtype=str)
workct=workct.set_index('blockid')
workloclist=sorted(workct.columns)
for i in workct.columns:
    workct[i]=pd.to_numeric(workct[i])
workct=workct.replace(999,np.nan)
workct['tractid']=[str(x)[0:11] for x in workct.index]
workct=workct.groupby(['tractid'])[workloclist].median(skipna=True)
workct.to_csv(path+'nyctract/workct.csv',index=True,na_rep='999')




# Block Level Gravity Model
# Res Gravity
resbkwac=pd.read_csv(path+'nyctract/resbk.csv',dtype=str)
resbkwac=resbkwac.set_index('blockid')
resloclist=sorted(resbkwac.columns)
wac=pd.DataFrame()
for i in ['ct','nj','ny','pa']:
    tp=pd.read_csv(path+'lehd/'+str(i)+'_wac_S000_JT03_2015.csv',dtype=str)
    tp=tp[['w_geocode','C000']]
    wac=pd.concat([wac,tp],axis=0)
wac.columns=['blockid','wac']
wac=wac.set_index('blockid')
resbkwac=pd.merge(resbkwac,wac,how='left',left_index=True,right_index=True)
resbkwac['wac']=resbkwac['wac'].replace(np.nan,'0')
for i in resbkwac.columns:
    resbkwac[i]=pd.to_numeric(resbkwac[i])
for i in resloclist:
    resbkwac[i]=np.where(resbkwac[i]<=10,5,
                np.where(resbkwac[i]<=20,15,
                np.where(resbkwac[i]<=30,25,
                np.where(resbkwac[i]<=40,35,
                np.where(resbkwac[i]<=50,45,
                np.where(resbkwac[i]<=60,55,
                np.nan))))))
resbkgravity=pd.DataFrame(index=resloclist,columns=['WAC1-10','WAC11-20','WAC21-30','WAC31-40','WAC41-50','WAC51-60',
                                                    'GWAC1-10','GWAC11-20','GWAC21-30','GWAC31-40','GWAC41-50','GWAC51-60',
                                                    'GRAVITYWAC'])
for i in resloclist:
    tp=sum(resbkwac.loc[resbkwac[i]==5,'wac'])
    resbkgravity.loc[i,'WAC1-10']=tp
    tp=sum(resbkwac.loc[resbkwac[i]==15,'wac'])
    resbkgravity.loc[i,'WAC11-20']=tp
    tp=sum(resbkwac.loc[resbkwac[i]==25,'wac'])
    resbkgravity.loc[i,'WAC21-30']=tp
    tp=sum(resbkwac.loc[resbkwac[i]==35,'wac'])
    resbkgravity.loc[i,'WAC31-40']=tp
    tp=sum(resbkwac.loc[resbkwac[i]==45,'wac'])
    resbkgravity.loc[i,'WAC41-50']=tp
    tp=sum(resbkwac.loc[resbkwac[i]==55,'wac'])
    resbkgravity.loc[i,'WAC51-60']=tp
    resbkgravity.loc[i,'GWAC1-10']=(resbkgravity.loc[i,'WAC1-10'])/(5**2)
    resbkgravity.loc[i,'GWAC11-20']=(resbkgravity.loc[i,'WAC11-20'])/(15**2)
    resbkgravity.loc[i,'GWAC21-30']=(resbkgravity.loc[i,'WAC21-30'])/(25**2)
    resbkgravity.loc[i,'GWAC31-40']=(resbkgravity.loc[i,'WAC31-40'])/(35**2)
    resbkgravity.loc[i,'GWAC41-50']=(resbkgravity.loc[i,'WAC41-50'])/(45**2)
    resbkgravity.loc[i,'GWAC51-60']=(resbkgravity.loc[i,'WAC51-60'])/(55**2)
    resbkgravity.loc[i,'GRAVITYWAC']=resbkgravity.loc[i,'GWAC1-10']+resbkgravity.loc[i,'GWAC11-20']+resbkgravity.loc[i,'GWAC21-30']+resbkgravity.loc[i,'GWAC31-40']+resbkgravity.loc[i,'GWAC41-50']+resbkgravity.loc[i,'GWAC51-60']
resbkgravity.to_csv(path+'nyctract/resbkgravity.csv',index=True)

# Work Gravity
workbkrac=pd.read_csv(path+'nyctract/workbk.csv',dtype=str)
workbkrac=workbkrac.set_index('blockid')
workloclist=sorted(workbkrac.columns)
rac=pd.DataFrame()
for i in ['ct','nj','ny','pa']:
    tp=pd.read_csv(path+'lehd/'+str(i)+'_rac_S000_JT03_2015.csv',dtype=str)
    tp=tp[['h_geocode','C000']]
    rac=pd.concat([rac,tp],axis=0)
rac.columns=['blockid','rac']
rac=rac.set_index('blockid')
workbkrac=pd.merge(workbkrac,rac,how='left',left_index=True,right_index=True)
workbkrac['rac']=workbkrac['rac'].replace(np.nan,'0')
for i in workbkrac.columns:
    workbkrac[i]=pd.to_numeric(workbkrac[i])
for i in workloclist:
    workbkrac[i]=np.where(workbkrac[i]<=10,5,
                 np.where(workbkrac[i]<=20,15,
                 np.where(workbkrac[i]<=30,25,
                 np.where(workbkrac[i]<=40,35,
                 np.where(workbkrac[i]<=50,45,
                 np.where(workbkrac[i]<=60,55,
                 np.nan))))))
workbkgravity=pd.DataFrame(index=workloclist,columns=['RAC1-10','RAC11-20','RAC21-30','RAC31-40','RAC41-50','RAC51-60',
                                                      'GRAC1-10','GRAC11-20','GRAC21-30','GRAC31-40','GRAC41-50','GRAC51-60',
                                                      'GRAVITYRAC'])
for i in workloclist:
    tp=sum(workbkrac.loc[workbkrac[i]==5,'rac'])
    workbkgravity.loc[i,'RAC1-10']=tp
    tp=sum(workbkrac.loc[workbkrac[i]==15,'rac'])
    workbkgravity.loc[i,'RAC11-20']=tp
    tp=sum(workbkrac.loc[workbkrac[i]==25,'rac'])
    workbkgravity.loc[i,'RAC21-30']=tp
    tp=sum(workbkrac.loc[workbkrac[i]==35,'rac'])
    workbkgravity.loc[i,'RAC31-40']=tp
    tp=sum(workbkrac.loc[workbkrac[i]==45,'rac'])
    workbkgravity.loc[i,'RAC41-50']=tp
    tp=sum(workbkrac.loc[workbkrac[i]==55,'rac'])
    workbkgravity.loc[i,'RAC51-60']=tp
    workbkgravity.loc[i,'GRAC1-10']=(workbkgravity.loc[i,'RAC1-10'])/(5**2)
    workbkgravity.loc[i,'GRAC11-20']=(workbkgravity.loc[i,'RAC11-20'])/(15**2)
    workbkgravity.loc[i,'GRAC21-30']=(workbkgravity.loc[i,'RAC21-30'])/(25**2)
    workbkgravity.loc[i,'GRAC31-40']=(workbkgravity.loc[i,'RAC31-40'])/(35**2)
    workbkgravity.loc[i,'GRAC41-50']=(workbkgravity.loc[i,'RAC41-50'])/(45**2)
    workbkgravity.loc[i,'GRAC51-60']=(workbkgravity.loc[i,'RAC51-60'])/(55**2)
    workbkgravity.loc[i,'GRAVITYRAC']=workbkgravity.loc[i,'GRAC1-10']+workbkgravity.loc[i,'GRAC11-20']+workbkgravity.loc[i,'GRAC21-30']+workbkgravity.loc[i,'GRAC31-40']+workbkgravity.loc[i,'GRAC41-50']+workbkgravity.loc[i,'GRAC51-60']
workbkgravity.to_csv(path+'nyctract/workbkgravity.csv',index=True)


## Tract Level Gravity Model
# Res Gravity
resctwac=pd.read_csv(path+'nyctract/resct.csv',dtype=str)
resctwac=resctwac.set_index('tractid')
resloclist=sorted(resctwac.columns)
wac=pd.DataFrame()
for i in ['ct','nj','ny','pa']:
    tp=pd.read_csv(path+'lehd/'+str(i)+'_wac_S000_JT03_2015.csv',dtype=str)
    tp=tp[['w_geocode','C000']]
    wac=pd.concat([wac,tp],axis=0)
wac.columns=['blockid','wac']
wac['tractid']=[str(x)[0:11] for x in wac['blockid']]
wac['wac']=pd.to_numeric(wac['wac'])
wac=pd.DataFrame(wac.groupby('tractid')['wac'].sum())
resctwac=pd.merge(resctwac,wac,how='left',left_index=True,right_index=True)
resctwac['wac']=resctwac['wac'].replace(np.nan,'0')
for i in resctwac.columns:
    resctwac[i]=pd.to_numeric(resctwac[i])
for i in resloclist:
    resctwac[i]=np.where(resctwac[i]<=10,5,
                np.where(resctwac[i]<=20,15,
                np.where(resctwac[i]<=30,25,
                np.where(resctwac[i]<=40,35,
                np.where(resctwac[i]<=50,45,
                np.where(resctwac[i]<=60,55,
                np.nan))))))
resctgravity=pd.DataFrame(index=resloclist,columns=['WAC1-10','WAC11-20','WAC21-30','WAC31-40','WAC41-50','WAC51-60',
                                                    'GWAC1-10','GWAC11-20','GWAC21-30','GWAC31-40','GWAC41-50','GWAC51-60',
                                                    'GRAVITYWAC'])
for i in resloclist:
    tp=sum(resctwac.loc[resctwac[i]==5,'wac'])
    resctgravity.loc[i,'WAC1-10']=tp
    tp=sum(resctwac.loc[resctwac[i]==15,'wac'])
    resctgravity.loc[i,'WAC11-20']=tp
    tp=sum(resctwac.loc[resctwac[i]==25,'wac'])
    resctgravity.loc[i,'WAC21-30']=tp
    tp=sum(resctwac.loc[resctwac[i]==35,'wac'])
    resctgravity.loc[i,'WAC31-40']=tp
    tp=sum(resctwac.loc[resctwac[i]==45,'wac'])
    resctgravity.loc[i,'WAC41-50']=tp
    tp=sum(resctwac.loc[resctwac[i]==55,'wac'])
    resctgravity.loc[i,'WAC51-60']=tp
    resctgravity.loc[i,'GWAC1-10']=(resctgravity.loc[i,'WAC1-10'])/(5**2)
    resctgravity.loc[i,'GWAC11-20']=(resctgravity.loc[i,'WAC11-20'])/(15**2)
    resctgravity.loc[i,'GWAC21-30']=(resctgravity.loc[i,'WAC21-30'])/(25**2)
    resctgravity.loc[i,'GWAC31-40']=(resctgravity.loc[i,'WAC31-40'])/(35**2)
    resctgravity.loc[i,'GWAC41-50']=(resctgravity.loc[i,'WAC41-50'])/(45**2)
    resctgravity.loc[i,'GWAC51-60']=(resctgravity.loc[i,'WAC51-60'])/(55**2)
    resctgravity.loc[i,'GRAVITYWAC']=resctgravity.loc[i,'GWAC1-10']+resctgravity.loc[i,'GWAC11-20']+resctgravity.loc[i,'GWAC21-30']+resctgravity.loc[i,'GWAC31-40']+resctgravity.loc[i,'GWAC41-50']+resctgravity.loc[i,'GWAC51-60']
resctgravity.to_csv(path+'nyctract/resctgravity.csv',index=True)

# Work Gravity
workctrac=pd.read_csv(path+'nyctract/workct.csv',dtype=str)
workctrac=workctrac.set_index('tractid')
workloclist=sorted(workctrac.columns)
rac=pd.DataFrame()
for i in ['ct','nj','ny','pa']:
    tp=pd.read_csv(path+'lehd/'+str(i)+'_rac_S000_JT03_2015.csv',dtype=str)
    tp=tp[['h_geocode','C000']]
    rac=pd.concat([rac,tp],axis=0)
rac.columns=['blockid','rac']
rac['tractid']=[str(x)[0:11] for x in rac['blockid']]
rac['rac']=pd.to_numeric(rac['rac'])
rac=pd.DataFrame(rac.groupby('tractid')['rac'].sum())
workctrac=pd.merge(workctrac,rac,how='left',left_index=True,right_index=True)
workctrac['rac']=workctrac['rac'].replace(np.nan,'0')
for i in workctrac.columns:
    workctrac[i]=pd.to_numeric(workctrac[i])
for i in workloclist:
    workctrac[i]=np.where(workctrac[i]<=10,5,
                 np.where(workctrac[i]<=20,15,
                 np.where(workctrac[i]<=30,25,
                 np.where(workctrac[i]<=40,35,
                 np.where(workctrac[i]<=50,45,
                 np.where(workctrac[i]<=60,55,
                 np.nan))))))
workctgravity=pd.DataFrame(index=workloclist,columns=['RAC1-10','RAC11-20','RAC21-30','RAC31-40','RAC41-50','RAC51-60',
                                                      'GRAC1-10','GRAC11-20','GRAC21-30','GRAC31-40','GRAC41-50','GRAC51-60',
                                                      'GRAVITYRAC'])
for i in workloclist:
    tp=sum(workctrac.loc[workctrac[i]==5,'rac'])
    workctgravity.loc[i,'RAC1-10']=tp
    tp=sum(workctrac.loc[workctrac[i]==15,'rac'])
    workctgravity.loc[i,'RAC11-20']=tp
    tp=sum(workctrac.loc[workctrac[i]==25,'rac'])
    workctgravity.loc[i,'RAC21-30']=tp
    tp=sum(workctrac.loc[workctrac[i]==35,'rac'])
    workctgravity.loc[i,'RAC31-40']=tp
    tp=sum(workctrac.loc[workctrac[i]==45,'rac'])
    workctgravity.loc[i,'RAC41-50']=tp
    tp=sum(workctrac.loc[workctrac[i]==55,'rac'])
    workctgravity.loc[i,'RAC51-60']=tp
    workctgravity.loc[i,'GRAC1-10']=(workctgravity.loc[i,'RAC1-10'])/(5**2)
    workctgravity.loc[i,'GRAC11-20']=(workctgravity.loc[i,'RAC11-20'])/(15**2)
    workctgravity.loc[i,'GRAC21-30']=(workctgravity.loc[i,'RAC21-30'])/(25**2)
    workctgravity.loc[i,'GRAC31-40']=(workctgravity.loc[i,'RAC31-40'])/(35**2)
    workctgravity.loc[i,'GRAC41-50']=(workctgravity.loc[i,'RAC41-50'])/(45**2)
    workctgravity.loc[i,'GRAC51-60']=(workctgravity.loc[i,'RAC51-60'])/(55**2)
    workctgravity.loc[i,'GRAVITYRAC']=workctgravity.loc[i,'GRAC1-10']+workctgravity.loc[i,'GRAC11-20']+workctgravity.loc[i,'GRAC21-30']+workctgravity.loc[i,'GRAC31-40']+workctgravity.loc[i,'GRAC41-50']+workctgravity.loc[i,'GRAC51-60']
workctgravity.to_csv(path+'nyctract/workctgravity.csv',index=True)









