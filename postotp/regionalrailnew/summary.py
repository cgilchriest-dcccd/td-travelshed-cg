#! /usr/bin/python3

import pandas as pd
import os
import numpy as np

#path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2018/TRAVELSHEDREVAMP/'
path='/home/mayijun/TRAVELSHED/'

nyc=['36005','36047','36061','36081','36085']



# Regional comparison
# Regionalrailnew
# Inbound
# wtgravity
gvbk=pd.read_csv(path+'regionalrailnew/inbound/wtbk.csv',dtype=str)
gvbk=gvbk.set_index('blockid')
loclist=gvbk.columns

rac=pd.DataFrame()
for i in ['ct','nj','ny','pa']:
    tp=pd.read_csv(path+'lehd/'+str(i)+'_rac_S000_JT03_2015.csv',dtype=str)
    tp=tp[['h_geocode','C000']]
    rac=pd.concat([rac,tp],axis=0)
rac.columns=['blockid','rac']
rac=rac.set_index('blockid')
gvbk=pd.merge(gvbk,rac,how='left',left_index=True,right_index=True)
gvbk.to_csv(path+'regionalrailnew/inbound/regionalrailnewinboundwtgravity.csv',index=True,na_rep='0')


gvbk=pd.read_csv(path+'regionalrailnew/inbound/regionalrailnewinboundwtgravity.csv',dtype=str)
gvbk=gvbk.set_index('blockid')
loclist=gvbk.columns[0:-1]
for i in loclist:
    gvbk[i]=pd.to_numeric(gvbk[i])
    gvbk[i]=np.where(gvbk[i]<=10,5,
            np.where(gvbk[i]<=20,15,
            np.where(gvbk[i]<=30,25,
            np.where(gvbk[i]<=40,35,
            np.where(gvbk[i]<=50,45,
            np.where(gvbk[i]<=60,55,
            np.nan))))))
gvbk['rac']=pd.to_numeric(gvbk['rac'])


locdetail=pd.read_excel(path+'regionalrailnew/inbound/input2.xlsx',sheet_name='input',dtype=str)
locdetail=locdetail.set_index('siteid')
locdetail=locdetail[['stopname','lat','long']]

gravitybk=pd.DataFrame(index=loclist,columns=['STOPNAME','LAT','LONG',
                                              'RAC1-10','RAC11-20','RAC21-30','RAC31-40','RAC41-50','RAC51-60',
                                              'RG1-10','RG11-20','RG21-30','RG31-40','RG41-50','RG51-60','RGRAVITY'])
for i in loclist:
    gravitybk.loc[i,'STOPNAME']=locdetail.loc[i,'stopname']
    gravitybk.loc[i,'LAT']=locdetail.loc[i,'lat']
    gravitybk.loc[i,'LONG']=locdetail.loc[i,'long']
    tp=sum(gvbk.loc[gvbk[i]==5,'rac'])
    gravitybk.loc[i,'RAC1-10']=tp
    tp=sum(gvbk.loc[gvbk[i]==15,'rac'])
    gravitybk.loc[i,'RAC11-20']=tp
    tp=sum(gvbk.loc[gvbk[i]==25,'rac'])
    gravitybk.loc[i,'RAC21-30']=tp
    tp=sum(gvbk.loc[gvbk[i]==35,'rac'])
    gravitybk.loc[i,'RAC31-40']=tp
    tp=sum(gvbk.loc[gvbk[i]==45,'rac'])
    gravitybk.loc[i,'RAC41-50']=tp
    tp=sum(gvbk.loc[gvbk[i]==55,'rac'])
    gravitybk.loc[i,'RAC51-60']=tp
    gravitybk.loc[i,'RG1-10']=gravitybk.loc[i,'RAC1-10']/(5**2)
    gravitybk.loc[i,'RG11-20']=gravitybk.loc[i,'RAC11-20']/(15**2)
    gravitybk.loc[i,'RG21-30']=gravitybk.loc[i,'RAC21-30']/(25**2)
    gravitybk.loc[i,'RG31-40']=gravitybk.loc[i,'RAC31-40']/(35**2)
    gravitybk.loc[i,'RG41-50']=gravitybk.loc[i,'RAC41-50']/(45**2)
    gravitybk.loc[i,'RG51-60']=gravitybk.loc[i,'RAC51-60']/(55**2)
    gravitybk.loc[i,'RGRAVITY']=gravitybk.loc[i,'RG1-10']+gravitybk.loc[i,'RG11-20']+gravitybk.loc[i,'RG21-30']+gravitybk.loc[i,'RG31-40']+gravitybk.loc[i,'RG41-50']+gravitybk.loc[i,'RG51-60']
gravitybk.to_csv(path+'regionalrailnew/inbound/regionalrailnewinboundwtgravity.csv',index=True)


# wtprgravity
gvbk=pd.read_csv(path+'regionalrailnew/inbound/wtprbk.csv',dtype=str)
gvbk=gvbk.set_index('blockid')
loclist=gvbk.columns

rac=pd.DataFrame()
for i in ['ct','nj','ny','pa']:
    tp=pd.read_csv(path+'lehd/'+str(i)+'_rac_S000_JT03_2015.csv',dtype=str)
    tp=tp[['h_geocode','C000']]
    rac=pd.concat([rac,tp],axis=0)
rac.columns=['blockid','rac']
rac=rac.set_index('blockid')
gvbk=pd.merge(gvbk,rac,how='left',left_index=True,right_index=True)
gvbk.to_csv(path+'regionalrailnew/inbound/regionalrailnewinboundwtprgravity.csv',index=True,na_rep='0')


gvbk=pd.read_csv(path+'regionalrailnew/inbound/regionalrailnewinboundwtprgravity.csv',dtype=str)
gvbk=gvbk.set_index('blockid')
loclist=gvbk.columns[0:-1]
for i in loclist:
    gvbk[i]=pd.to_numeric(gvbk[i])
    gvbk[i]=np.where(gvbk[i]<=10,5,
            np.where(gvbk[i]<=20,15,
            np.where(gvbk[i]<=30,25,
            np.where(gvbk[i]<=40,35,
            np.where(gvbk[i]<=50,45,
            np.where(gvbk[i]<=60,55,
            np.nan))))))
gvbk['rac']=pd.to_numeric(gvbk['rac'])


locdetail=pd.read_excel(path+'regionalrailnew/inbound/input2.xlsx',sheet_name='input',dtype=str)
locdetail=locdetail.set_index('siteid')
locdetail=locdetail[['stopname','lat','long']]

gravitybk=pd.DataFrame(index=loclist,columns=['STOPNAME','LAT','LONG',
                                              'RAC1-10','RAC11-20','RAC21-30','RAC31-40','RAC41-50','RAC51-60',
                                              'RG1-10','RG11-20','RG21-30','RG31-40','RG41-50','RG51-60','RGRAVITY'])
for i in loclist:
    gravitybk.loc[i,'STOPNAME']=locdetail.loc[i,'stopname']
    gravitybk.loc[i,'LAT']=locdetail.loc[i,'lat']
    gravitybk.loc[i,'LONG']=locdetail.loc[i,'long']
    tp=sum(gvbk.loc[gvbk[i]==5,'rac'])
    gravitybk.loc[i,'RAC1-10']=tp
    tp=sum(gvbk.loc[gvbk[i]==15,'rac'])
    gravitybk.loc[i,'RAC11-20']=tp
    tp=sum(gvbk.loc[gvbk[i]==25,'rac'])
    gravitybk.loc[i,'RAC21-30']=tp
    tp=sum(gvbk.loc[gvbk[i]==35,'rac'])
    gravitybk.loc[i,'RAC31-40']=tp
    tp=sum(gvbk.loc[gvbk[i]==45,'rac'])
    gravitybk.loc[i,'RAC41-50']=tp
    tp=sum(gvbk.loc[gvbk[i]==55,'rac'])
    gravitybk.loc[i,'RAC51-60']=tp
    gravitybk.loc[i,'RG1-10']=gravitybk.loc[i,'RAC1-10']/(5**2)
    gravitybk.loc[i,'RG11-20']=gravitybk.loc[i,'RAC11-20']/(15**2)
    gravitybk.loc[i,'RG21-30']=gravitybk.loc[i,'RAC21-30']/(25**2)
    gravitybk.loc[i,'RG31-40']=gravitybk.loc[i,'RAC31-40']/(35**2)
    gravitybk.loc[i,'RG41-50']=gravitybk.loc[i,'RAC41-50']/(45**2)
    gravitybk.loc[i,'RG51-60']=gravitybk.loc[i,'RAC51-60']/(55**2)
    gravitybk.loc[i,'RGRAVITY']=gravitybk.loc[i,'RG1-10']+gravitybk.loc[i,'RG11-20']+gravitybk.loc[i,'RG21-30']+gravitybk.loc[i,'RG31-40']+gravitybk.loc[i,'RG41-50']+gravitybk.loc[i,'RG51-60']
gravitybk.to_csv(path+'regionalrailnew/inbound/regionalrailnewinboundwtprgravity.csv',index=True)



# Outbound
# wtgravity
gvbk=pd.read_csv(path+'regionalrailnew/outbound/wtbk.csv',dtype=str)
gvbk=gvbk.set_index('blockid')
loclist=gvbk.columns

wac=pd.DataFrame()
for i in ['ct','nj','ny','pa']:
    tp=pd.read_csv(path+'lehd/'+str(i)+'_wac_S000_JT03_2015.csv',dtype=str)
    tp=tp[['w_geocode','C000']]
    wac=pd.concat([wac,tp],axis=0)
wac.columns=['blockid','wac']
wac=wac.set_index('blockid')
gvbk=pd.merge(gvbk,wac,how='left',left_index=True,right_index=True)
gvbk.to_csv(path+'regionalrailnew/outbound/regionalrailnewoutboundwtgravity.csv',index=True,na_rep='0')


gvbk=pd.read_csv(path+'regionalrailnew/outbound/regionalrailnewoutboundwtgravity.csv',dtype=str)
gvbk=gvbk.set_index('blockid')
loclist=gvbk.columns[0:-1]
for i in loclist:
    gvbk[i]=pd.to_numeric(gvbk[i])
    gvbk[i]=np.where(gvbk[i]<=10,5,
            np.where(gvbk[i]<=20,15,
            np.where(gvbk[i]<=30,25,
            np.where(gvbk[i]<=40,35,
            np.where(gvbk[i]<=50,45,
            np.where(gvbk[i]<=60,55,
            np.nan))))))
gvbk['wac']=pd.to_numeric(gvbk['wac'])


locdetail=pd.read_excel(path+'regionalrailnew/outbound/input2.xlsx',sheet_name='input',dtype=str)
locdetail=locdetail.set_index('siteid')
locdetail=locdetail[['stopname','lat','long']]

gravitybk=pd.DataFrame(index=loclist,columns=['STOPNAME','LAT','LONG',
                                              'WAC1-10','WAC11-20','WAC21-30','WAC31-40','WAC41-50','WAC51-60',
                                              'WG1-10','WG11-20','WG21-30','WG31-40','WG41-50','WG51-60','WGRAVITY'])
for i in loclist:
    gravitybk.loc[i,'STOPNAME']=locdetail.loc[i,'stopname']
    gravitybk.loc[i,'LAT']=locdetail.loc[i,'lat']
    gravitybk.loc[i,'LONG']=locdetail.loc[i,'long']
    tp=sum(gvbk.loc[gvbk[i]==5,'wac'])
    gravitybk.loc[i,'WAC1-10']=tp
    tp=sum(gvbk.loc[gvbk[i]==15,'wac'])
    gravitybk.loc[i,'WAC11-20']=tp
    tp=sum(gvbk.loc[gvbk[i]==25,'wac'])
    gravitybk.loc[i,'WAC21-30']=tp
    tp=sum(gvbk.loc[gvbk[i]==35,'wac'])
    gravitybk.loc[i,'WAC31-40']=tp
    tp=sum(gvbk.loc[gvbk[i]==45,'wac'])
    gravitybk.loc[i,'WAC41-50']=tp
    tp=sum(gvbk.loc[gvbk[i]==55,'wac'])
    gravitybk.loc[i,'WAC51-60']=tp
    gravitybk.loc[i,'WG1-10']=gravitybk.loc[i,'WAC1-10']/(5**2)
    gravitybk.loc[i,'WG11-20']=gravitybk.loc[i,'WAC11-20']/(15**2)
    gravitybk.loc[i,'WG21-30']=gravitybk.loc[i,'WAC21-30']/(25**2)
    gravitybk.loc[i,'WG31-40']=gravitybk.loc[i,'WAC31-40']/(35**2)
    gravitybk.loc[i,'WG41-50']=gravitybk.loc[i,'WAC41-50']/(45**2)
    gravitybk.loc[i,'WG51-60']=gravitybk.loc[i,'WAC51-60']/(55**2)
    gravitybk.loc[i,'WGRAVITY']=gravitybk.loc[i,'WG1-10']+gravitybk.loc[i,'WG11-20']+gravitybk.loc[i,'WG21-30']+gravitybk.loc[i,'WG31-40']+gravitybk.loc[i,'WG41-50']+gravitybk.loc[i,'WG51-60']
gravitybk.to_csv(path+'regionalrailnew/outbound/regionalrailnewoutboundwtgravity.csv',index=True)


# wtprgravity
gvbk=pd.read_csv(path+'regionalrailnew/outbound/wtprbk.csv',dtype=str)
gvbk=gvbk.set_index('blockid')
loclist=gvbk.columns

wac=pd.DataFrame()
for i in ['ct','nj','ny','pa']:
    tp=pd.read_csv(path+'lehd/'+str(i)+'_wac_S000_JT03_2015.csv',dtype=str)
    tp=tp[['w_geocode','C000']]
    wac=pd.concat([wac,tp],axis=0)
wac.columns=['blockid','wac']
wac=wac.set_index('blockid')
gvbk=pd.merge(gvbk,wac,how='left',left_index=True,right_index=True)
gvbk.to_csv(path+'regionalrailnew/outbound/regionalrailnewoutboundwtprgravity.csv',index=True,na_rep='0')


gvbk=pd.read_csv(path+'regionalrailnew/outbound/regionalrailnewoutboundwtprgravity.csv',dtype=str)
gvbk=gvbk.set_index('blockid')
loclist=gvbk.columns[0:-1]
for i in loclist:
    gvbk[i]=pd.to_numeric(gvbk[i])
    gvbk[i]=np.where(gvbk[i]<=10,5,
            np.where(gvbk[i]<=20,15,
            np.where(gvbk[i]<=30,25,
            np.where(gvbk[i]<=40,35,
            np.where(gvbk[i]<=50,45,
            np.where(gvbk[i]<=60,55,
            np.nan))))))
gvbk['wac']=pd.to_numeric(gvbk['wac'])


locdetail=pd.read_excel(path+'regionalrailnew/outbound/input2.xlsx',sheet_name='input',dtype=str)
locdetail=locdetail.set_index('siteid')
locdetail=locdetail[['stopname','lat','long']]

gravitybk=pd.DataFrame(index=loclist,columns=['STOPNAME','LAT','LONG',
                                              'WAC1-10','WAC11-20','WAC21-30','WAC31-40','WAC41-50','WAC51-60',
                                              'WG1-10','WG11-20','WG21-30','WG31-40','WG41-50','WG51-60','WGRAVITY'])
for i in loclist:
    gravitybk.loc[i,'STOPNAME']=locdetail.loc[i,'stopname']
    gravitybk.loc[i,'LAT']=locdetail.loc[i,'lat']
    gravitybk.loc[i,'LONG']=locdetail.loc[i,'long']
    tp=sum(gvbk.loc[gvbk[i]==5,'wac'])
    gravitybk.loc[i,'WAC1-10']=tp
    tp=sum(gvbk.loc[gvbk[i]==15,'wac'])
    gravitybk.loc[i,'WAC11-20']=tp
    tp=sum(gvbk.loc[gvbk[i]==25,'wac'])
    gravitybk.loc[i,'WAC21-30']=tp
    tp=sum(gvbk.loc[gvbk[i]==35,'wac'])
    gravitybk.loc[i,'WAC31-40']=tp
    tp=sum(gvbk.loc[gvbk[i]==45,'wac'])
    gravitybk.loc[i,'WAC41-50']=tp
    tp=sum(gvbk.loc[gvbk[i]==55,'wac'])
    gravitybk.loc[i,'WAC51-60']=tp
    gravitybk.loc[i,'WG1-10']=gravitybk.loc[i,'WAC1-10']/(5**2)
    gravitybk.loc[i,'WG11-20']=gravitybk.loc[i,'WAC11-20']/(15**2)
    gravitybk.loc[i,'WG21-30']=gravitybk.loc[i,'WAC21-30']/(25**2)
    gravitybk.loc[i,'WG31-40']=gravitybk.loc[i,'WAC31-40']/(35**2)
    gravitybk.loc[i,'WG41-50']=gravitybk.loc[i,'WAC41-50']/(45**2)
    gravitybk.loc[i,'WG51-60']=gravitybk.loc[i,'WAC51-60']/(55**2)
    gravitybk.loc[i,'WGRAVITY']=gravitybk.loc[i,'WG1-10']+gravitybk.loc[i,'WG11-20']+gravitybk.loc[i,'WG21-30']+gravitybk.loc[i,'WG31-40']+gravitybk.loc[i,'WG41-50']+gravitybk.loc[i,'WG51-60']
gravitybk.to_csv(path+'regionalrailnew/outbound/regionalrailnewoutboundwtprgravity.csv',index=True)
