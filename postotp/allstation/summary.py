#! /usr/bin/python3

import pandas as pd
import os
import numpy as np

#path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2018/TRAVELSHEDREVAMP/'
path='/home/mayijun/TRAVELSHED/'

nyc=['36005','36047','36061','36081','36085']


## Walk and Transit Summary
#df=pd.DataFrame()
#for i in os.listdir(path+'wt'):
#    tp=pd.read_csv(path+'wt/'+i,dtype=str)
#    tp=tp.set_index('blockid')
#    df=pd.concat([df,tp],axis=1)
#df.to_csv(path+'wt/wtbk.csv',index=True)
#
#wtbk=pd.read_csv(path+'wt/wtbk.csv',dtype=str)
#wtbk=wtbk.set_index('blockid')
#loclist=sorted(wtbk.columns)
#for i in wtbk.columns:
#    wtbk[i]=pd.to_numeric(wtbk[i])
#wtbk=wtbk.replace(999,np.nan)
#wtbk['tractid']=[str(x)[0:11] for x in wtbk.index]
#wtbk=wtbk.groupby(['tractid'])[loclist].median(skipna=True)
#wtbk.to_csv(path+'wt/wtct.csv',index=True,na_rep='999')
#
#
#
## Park and Ride Summary
#df=pd.DataFrame()
#for i in os.listdir(path+'pr'):
#    tp=pd.read_csv(path+'pr/'+i,dtype=str)
#    tp=tp.set_index('blockid')
#    df=pd.concat([df,tp],axis=1)
#df.to_csv(path+'pr/prbk.csv',index=True)
#
#prbk=pd.read_csv(path+'pr/prbk.csv',dtype=str)
#prbk=prbk.set_index('blockid')
#loclist=sorted(prbk.columns)
#for i in prbk.columns:
#    prbk[i]=pd.to_numeric(prbk[i])
#prbk=prbk.replace(999,np.nan)
#prbk['tractid']=[str(x)[0:11] for x in prbk.index]
#prbk=prbk.groupby(['tractid'])[loclist].median(skipna=True)
#prbk.to_csv(path+'pr/prct.csv',index=True,na_rep='999')


## Adjust Walk and Transit Summary
#adj=pd.read_csv(path+'allstation/wt/ADJSUBWAY197.csv',dtype=str)
#adj=adj.set_index('blockid')
#df=pd.read_csv(path+'allstation/wt/wtbk.csv',dtype=str)
#df=df.set_index('blockid')
#loclist=df.columns
#df=pd.concat([df,adj],axis=1)
#df['SUBWAY197']=df['ADJSUBWAY197']
#df=df[loclist]
#df.to_csv(path+'allstation/wt/wtbk2.csv',index=True)
#
#wtbk=pd.read_csv(path+'allstation/wt/wtbk2.csv',dtype=str)
#wtbk=wtbk.set_index('blockid')
#loclist=sorted(wtbk.columns)
#for i in wtbk.columns:
#    wtbk[i]=pd.to_numeric(wtbk[i])
#wtbk=wtbk.replace(999,np.nan)
#wtbk['tractid']=[str(x)[0:11] for x in wtbk.index]
#wtbk=wtbk.groupby(['tractid'])[loclist].median(skipna=True)
#wtbk.to_csv(path+'allstation/wt/wtct2.csv',index=True,na_rep='999')



## Adjust Park and Ride Summary
#adj=pd.read_csv(path+'allstation/pr/ADJSUBWAY197.csv',dtype=str)
#adj=adj.set_index('blockid')
#df=pd.read_csv(path+'allstation/pr/prbk.csv',dtype=str)
#df=df.set_index('blockid')
#loclist=df.columns
#df=pd.concat([df,adj],axis=1)
#df['SUBWAY197']=df['ADJSUBWAY197']
#df=df[loclist]
#df.to_csv(path+'allstation/pr/prbk2.csv',index=True)
#
#prbk=pd.read_csv(path+'allstation/pr/prbk2.csv',dtype=str)
#prbk=prbk.set_index('blockid')
#loclist=sorted(prbk.columns)
#for i in prbk.columns:
#    prbk[i]=pd.to_numeric(prbk[i])
#prbk=prbk.replace(999,np.nan)
#prbk['tractid']=[str(x)[0:11] for x in prbk.index]
#prbk=prbk.groupby(['tractid'])[loclist].median(skipna=True)
#prbk.to_csv(path+'allstation/pr/prct2.csv',index=True,na_rep='999')
#
#
#
#
## Combine Walk and Transit and Park and Ride
#wt=pd.read_csv(path+'allstation/wt/wtbk2.csv',dtype=str)
#wt=wt.set_index('blockid')
#loclist=sorted(wt.columns)
#wt.columns=['WT'+str(x) for x in wt.columns]
#
#pr=pd.read_csv(path+'allstation/pr/prbk2.csv',dtype=str)
#pr=pr.set_index('blockid')
#pr.columns=['PR'+str(x) for x in pr.columns]
#
#tsbk=pd.concat([wt,pr],axis=1)
#for i in tsbk.columns:
#    tsbk[i]=pd.to_numeric(tsbk[i])
#for i in loclist:
#    tsbk[i]=np.where([str(x)[0:5] not in nyc for x in tsbk.index],
#          pd.concat([tsbk['WT'+str(i)],tsbk['PR'+str(i)]],axis=1).min(axis=1),
#          tsbk['WT'+str(i)])
#tsbk=tsbk[loclist]
#tsbk.to_csv(path+'allstation/travelshedbk2.csv',index=True)
#
#tsct=pd.read_csv(path+'allstation/travelshedbk2.csv',dtype=str)
#tsct=tsct.set_index('blockid')
#for i in tsct.columns:
#    tsct[i]=pd.to_numeric(tsct[i])
#tsct=tsct.replace(999,np.nan)
#tsct['tractid']=[str(x)[0:11] for x in tsct.index]
#tsct=tsct.groupby(['tractid'])[loclist].median(skipna=True)
#tsct.to_csv(path+'allstation/travelshedct2.csv',index=True,na_rep='999')
#
#
#
## Gravity Model
#gvbk=pd.read_csv(path+'allstation/travelshedbk2.csv',dtype=str)
#gvbk=gvbk.set_index('blockid')
#loclist=sorted(gvbk.columns)
#
#rac=pd.DataFrame()
#for i in ['ct','nj','ny','pa']:
#    tp=pd.read_csv(path+'lehd/'+str(i)+'_rac_S000_JT03_2015.csv',dtype=str)
#    tp=tp[['h_geocode','C000']]
#    rac=pd.concat([rac,tp],axis=0)
#rac.columns=['blockid','rac']
#rac=rac.set_index('blockid')
#gvbk=pd.merge(gvbk,rac,how='left',left_index=True,right_index=True)
#
#wac=pd.DataFrame()
#for i in ['ct','nj','ny','pa']:
#    tp=pd.read_csv(path+'lehd/'+str(i)+'_wac_S000_JT03_2015.csv',dtype=str)
#    tp=tp[['w_geocode','C000']]
#    wac=pd.concat([wac,tp],axis=0)
#wac.columns=['blockid','wac']
#wac=wac.set_index('blockid')
#gvbk=pd.merge(gvbk,wac,how='left',left_index=True,right_index=True)
#
#gvbk.to_csv(path+'allstation/gravitybk2.csv',index=True,na_rep='0')
#gvbk=pd.read_csv(path+'allstation/gravitybk2.csv',dtype=str)
#for i in loclist:
#    gvbk[i]=pd.to_numeric(gvbk[i])
#    gvbk[i]=np.where(gvbk[i]<=10,5,
#            np.where(gvbk[i]<=20,15,
#            np.where(gvbk[i]<=30,25,
#            np.where(gvbk[i]<=40,35,
#            np.where(gvbk[i]<=50,45,
#            np.where(gvbk[i]<=60,55,
#            np.nan))))))
#gvbk['rac']=pd.to_numeric(gvbk['rac'])
#gvbk['wac']=pd.to_numeric(gvbk['wac'])
#
#locdetail=pd.read_csv(path+'location/location.csv',dtype=str)
#locdetail=locdetail.set_index('locationid')
#locdetail=locdetail[['type','boro','name','routes','intlat','intlong']]
#
#gravitybk=pd.DataFrame(index=loclist,columns=['TYPE','BORO','NAME','ROUTES','INTLAT','INTLONG',
#                                              'RAC1-10','RAC11-20','RAC21-30','RAC31-40','RAC41-50','RAC51-60',
#                                              'WAC1-10','WAC11-20','WAC21-30','WAC31-40','WAC41-50','WAC51-60',
#                                              'G1-10','G11-20','G21-30','G31-40','G41-50','G51-60','GRAVITY'])
#for i in loclist:
#    gravitybk.loc[i,'TYPE']=locdetail.loc[i,'type']
#    gravitybk.loc[i,'BORO']=locdetail.loc[i,'boro']
#    gravitybk.loc[i,'NAME']=locdetail.loc[i,'name']
#    gravitybk.loc[i,'ROUTES']=locdetail.loc[i,'routes']
#    gravitybk.loc[i,'INTLAT']=locdetail.loc[i,'intlat']
#    gravitybk.loc[i,'INTLONG']=locdetail.loc[i,'intlong']
#    tp=sum(gvbk.loc[gvbk[i]==5,'rac'])
#    gravitybk.loc[i,'RAC1-10']=tp
#    tp=sum(gvbk.loc[gvbk[i]==15,'rac'])
#    gravitybk.loc[i,'RAC11-20']=tp
#    tp=sum(gvbk.loc[gvbk[i]==25,'rac'])
#    gravitybk.loc[i,'RAC21-30']=tp
#    tp=sum(gvbk.loc[gvbk[i]==35,'rac'])
#    gravitybk.loc[i,'RAC31-40']=tp
#    tp=sum(gvbk.loc[gvbk[i]==45,'rac'])
#    gravitybk.loc[i,'RAC41-50']=tp
#    tp=sum(gvbk.loc[gvbk[i]==55,'rac'])
#    gravitybk.loc[i,'RAC51-60']=tp
#    tp=sum(gvbk.loc[gvbk[i]==5,'wac'])
#    gravitybk.loc[i,'WAC1-10']=tp
#    tp=sum(gvbk.loc[gvbk[i]==15,'wac'])
#    gravitybk.loc[i,'WAC11-20']=tp
#    tp=sum(gvbk.loc[gvbk[i]==25,'wac'])
#    gravitybk.loc[i,'WAC21-30']=tp
#    tp=sum(gvbk.loc[gvbk[i]==35,'wac'])
#    gravitybk.loc[i,'WAC31-40']=tp
#    tp=sum(gvbk.loc[gvbk[i]==45,'wac'])
#    gravitybk.loc[i,'WAC41-50']=tp
#    tp=sum(gvbk.loc[gvbk[i]==55,'wac'])
#    gravitybk.loc[i,'WAC51-60']=tp
#    gravitybk.loc[i,'G1-10']=(gravitybk.loc[i,'RAC1-10']*gravitybk.loc[i,'WAC1-10'])/(5**2)
#    gravitybk.loc[i,'G11-20']=(gravitybk.loc[i,'RAC11-20']*gravitybk.loc[i,'WAC11-20'])/(15**2)
#    gravitybk.loc[i,'G21-30']=(gravitybk.loc[i,'RAC21-30']*gravitybk.loc[i,'WAC21-30'])/(25**2)
#    gravitybk.loc[i,'G31-40']=(gravitybk.loc[i,'RAC31-40']*gravitybk.loc[i,'WAC31-40'])/(35**2)
#    gravitybk.loc[i,'G41-50']=(gravitybk.loc[i,'RAC41-50']*gravitybk.loc[i,'WAC41-50'])/(45**2)
#    gravitybk.loc[i,'G51-60']=(gravitybk.loc[i,'RAC51-60']*gravitybk.loc[i,'WAC51-60'])/(55**2)
#    gravitybk.loc[i,'GRAVITY']=gravitybk.loc[i,'G1-10']+gravitybk.loc[i,'G11-20']+gravitybk.loc[i,'G21-30']+gravitybk.loc[i,'G31-40']+gravitybk.loc[i,'G41-50']+gravitybk.loc[i,'G51-60']
#gravitybk.to_csv(path+'allstation/gravitybk2.csv',index=True)


















## Infobrief
#
## Gravity Model
#gvbk=pd.read_csv(path+'allstation/inbound/wt/wtbk3.csv',dtype=str)
#gvbk=gvbk.set_index('blockid')
#loclist=gvbk.columns
#
#rac=pd.DataFrame()
#for i in ['ct','nj','ny','pa']:
#    tp=pd.read_csv(path+'lehd/'+str(i)+'_rac_S000_JT03_2015.csv',dtype=str)
#    tp=tp[['h_geocode','C000']]
#    rac=pd.concat([rac,tp],axis=0)
#rac.columns=['blockid','rac']
#rac=rac.set_index('blockid')
#gvbk=pd.merge(gvbk,rac,how='left',left_index=True,right_index=True)
#
#wac=pd.DataFrame()
#for i in ['ct','nj','ny','pa']:
#    tp=pd.read_csv(path+'lehd/'+str(i)+'_wac_S000_JT03_2015.csv',dtype=str)
#    tp=tp[['w_geocode','C000']]
#    wac=pd.concat([wac,tp],axis=0)
#wac.columns=['blockid','wac']
#wac=wac.set_index('blockid')
#gvbk=pd.merge(gvbk,wac,how='left',left_index=True,right_index=True)
#gvbk.to_csv(path+'allstation/inbound/gravitybk4.csv',index=True,na_rep='0')
#
#
#gvbk=pd.read_csv(path+'allstation/inbound/gravitybk4.csv',dtype=str)
#gvbk=gvbk.set_index('blockid')
#loclist=gvbk.columns[0:-2]
#for i in loclist:
#    gvbk[i]=pd.to_numeric(gvbk[i])
#    gvbk[i]=np.where(gvbk[i]<=10,5,
#            np.where(gvbk[i]<=20,15,
#            np.where(gvbk[i]<=30,25,
#            np.where(gvbk[i]<=40,35,
#            np.where(gvbk[i]<=50,45,
#            np.where(gvbk[i]<=60,55,
#            np.nan))))))
#gvbk['rac']=pd.to_numeric(gvbk['rac'])
#gvbk['wac']=pd.to_numeric(gvbk['wac'])
#
#
#locdetail=pd.read_excel(path+'allstation/inbound/location.xlsx',sheet_name='location',dtype=str)
#locdetail=locdetail.set_index('newlocationid')
#locdetail=locdetail[['type','boro','name','routes','intlat','intlong']]
#
#gravitybk=pd.DataFrame(index=loclist,columns=['TYPE','BORO','NAME','ROUTES','INTLAT','INTLONG',
#                                              'RAC1-10','RAC11-20','RAC21-30','RAC31-40','RAC41-50','RAC51-60',
#                                              'WAC1-10','WAC11-20','WAC21-30','WAC31-40','WAC41-50','WAC51-60',
#                                              'RG1-10','RG11-20','RG21-30','RG31-40','RG41-50','RG51-60','RGRAVITY','RRANK','RPCT'])
#for i in loclist:
#    gravitybk.loc[i,'TYPE']=locdetail.loc[i,'type']
#    gravitybk.loc[i,'BORO']=locdetail.loc[i,'boro']
#    gravitybk.loc[i,'NAME']=locdetail.loc[i,'name']
#    gravitybk.loc[i,'ROUTES']=locdetail.loc[i,'routes']
#    gravitybk.loc[i,'INTLAT']=locdetail.loc[i,'intlat']
#    gravitybk.loc[i,'INTLONG']=locdetail.loc[i,'intlong']
#    tp=sum(gvbk.loc[gvbk[i]==5,'rac'])
#    gravitybk.loc[i,'RAC1-10']=tp
#    tp=sum(gvbk.loc[gvbk[i]==15,'rac'])
#    gravitybk.loc[i,'RAC11-20']=tp
#    tp=sum(gvbk.loc[gvbk[i]==25,'rac'])
#    gravitybk.loc[i,'RAC21-30']=tp
#    tp=sum(gvbk.loc[gvbk[i]==35,'rac'])
#    gravitybk.loc[i,'RAC31-40']=tp
#    tp=sum(gvbk.loc[gvbk[i]==45,'rac'])
#    gravitybk.loc[i,'RAC41-50']=tp
#    tp=sum(gvbk.loc[gvbk[i]==55,'rac'])
#    gravitybk.loc[i,'RAC51-60']=tp
#    tp=sum(gvbk.loc[gvbk[i]==5,'wac'])
#    gravitybk.loc[i,'WAC1-10']=tp
#    tp=sum(gvbk.loc[gvbk[i]==15,'wac'])
#    gravitybk.loc[i,'WAC11-20']=tp
#    tp=sum(gvbk.loc[gvbk[i]==25,'wac'])
#    gravitybk.loc[i,'WAC21-30']=tp
#    tp=sum(gvbk.loc[gvbk[i]==35,'wac'])
#    gravitybk.loc[i,'WAC31-40']=tp
#    tp=sum(gvbk.loc[gvbk[i]==45,'wac'])
#    gravitybk.loc[i,'WAC41-50']=tp
#    tp=sum(gvbk.loc[gvbk[i]==55,'wac'])
#    gravitybk.loc[i,'WAC51-60']=tp
#    gravitybk.loc[i,'RG1-10']=gravitybk.loc[i,'RAC1-10']/(5**2)
#    gravitybk.loc[i,'RG11-20']=gravitybk.loc[i,'RAC11-20']/(15**2)
#    gravitybk.loc[i,'RG21-30']=gravitybk.loc[i,'RAC21-30']/(25**2)
#    gravitybk.loc[i,'RG31-40']=gravitybk.loc[i,'RAC31-40']/(35**2)
#    gravitybk.loc[i,'RG41-50']=gravitybk.loc[i,'RAC41-50']/(45**2)
#    gravitybk.loc[i,'RG51-60']=gravitybk.loc[i,'RAC51-60']/(55**2)
#    gravitybk.loc[i,'RGRAVITY']=gravitybk.loc[i,'RG1-10']+gravitybk.loc[i,'RG11-20']+gravitybk.loc[i,'RG21-30']+gravitybk.loc[i,'RG31-40']+gravitybk.loc[i,'RG41-50']+gravitybk.loc[i,'RG51-60']
#gravitybk['RRANK']=gravitybk['RGRAVITY'].rank(ascending=False)
#gravitybk['RPCT']=gravitybk['RGRAVITY'].rank(ascending=False,pct=True)
#gravitybk.to_csv(path+'allstation/inbound/gravitybk4.csv',index=True)
#

















# Regional comparison
# All station
# Inbound
# wtgravity
gvbk=pd.read_csv(path+'allstation/inbound/wt/wtbk3.csv',dtype=str)
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
gvbk.to_csv(path+'allstation/inbound/allstationinboundwtgravity.csv',index=True,na_rep='0')


gvbk=pd.read_csv(path+'allstation/inbound/allstationinboundwtgravity.csv',dtype=str)
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


locdetail=pd.read_excel(path+'allstation/inbound/location.xlsx',sheet_name='location',dtype=str)
locdetail=locdetail.set_index('newlocationid')
locdetail=locdetail[['type','boro','name','routes','intlat','intlong']]

gravitybk=pd.DataFrame(index=loclist,columns=['TYPE','BORO','NAME','ROUTES','INTLAT','INTLONG',
                                              'RAC1-10','RAC11-20','RAC21-30','RAC31-40','RAC41-50','RAC51-60',
                                              'RG1-10','RG11-20','RG21-30','RG31-40','RG41-50','RG51-60','RGRAVITY'])
for i in loclist:
    gravitybk.loc[i,'TYPE']=locdetail.loc[i,'type']
    gravitybk.loc[i,'BORO']=locdetail.loc[i,'boro']
    gravitybk.loc[i,'NAME']=locdetail.loc[i,'name']
    gravitybk.loc[i,'ROUTES']=locdetail.loc[i,'routes']
    gravitybk.loc[i,'INTLAT']=locdetail.loc[i,'intlat']
    gravitybk.loc[i,'INTLONG']=locdetail.loc[i,'intlong']
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
gravitybk.to_csv(path+'allstation/inbound/allstationinboundwtgravity.csv',index=True)


# wtprgravity
gvbk=pd.read_csv(path+'allstation/inbound/travelshedbk3.csv',dtype=str)
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
gvbk.to_csv(path+'allstation/inbound/allstationinboundwtprgravity.csv',index=True,na_rep='0')


gvbk=pd.read_csv(path+'allstation/inbound/allstationinboundwtprgravity.csv',dtype=str)
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


locdetail=pd.read_excel(path+'allstation/inbound/location.xlsx',sheet_name='location',dtype=str)
locdetail=locdetail.set_index('newlocationid')
locdetail=locdetail[['type','boro','name','routes','intlat','intlong']]

gravitybk=pd.DataFrame(index=loclist,columns=['TYPE','BORO','NAME','ROUTES','INTLAT','INTLONG',
                                              'RAC1-10','RAC11-20','RAC21-30','RAC31-40','RAC41-50','RAC51-60',
                                              'RG1-10','RG11-20','RG21-30','RG31-40','RG41-50','RG51-60','RGRAVITY'])
for i in loclist:
    gravitybk.loc[i,'TYPE']=locdetail.loc[i,'type']
    gravitybk.loc[i,'BORO']=locdetail.loc[i,'boro']
    gravitybk.loc[i,'NAME']=locdetail.loc[i,'name']
    gravitybk.loc[i,'ROUTES']=locdetail.loc[i,'routes']
    gravitybk.loc[i,'INTLAT']=locdetail.loc[i,'intlat']
    gravitybk.loc[i,'INTLONG']=locdetail.loc[i,'intlong']
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
gravitybk.to_csv(path+'allstation/inbound/allstationinboundwtprgravity.csv',index=True)



# Outbound
# wtgravity
gvbk=pd.read_csv(path+'allstation/outbound/wtbk.csv',dtype=str)
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
gvbk.to_csv(path+'allstation/outbound/allstationoutboundwtgravity.csv',index=True,na_rep='0')


gvbk=pd.read_csv(path+'allstation/outbound/allstationoutboundwtgravity.csv',dtype=str)
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


locdetail=pd.read_excel(path+'allstation/inbound/location.xlsx',sheet_name='location',dtype=str)
locdetail=locdetail.set_index('newlocationid')
locdetail=locdetail[['type','boro','name','routes','intlat','intlong']]

gravitybk=pd.DataFrame(index=loclist,columns=['TYPE','BORO','NAME','ROUTES','INTLAT','INTLONG',
                                              'WAC1-10','WAC11-20','WAC21-30','WAC31-40','WAC41-50','WAC51-60',
                                              'WG1-10','WG11-20','WG21-30','WG31-40','WG41-50','WG51-60','WGRAVITY'])
for i in loclist:
    gravitybk.loc[i,'TYPE']=locdetail.loc[i,'type']
    gravitybk.loc[i,'BORO']=locdetail.loc[i,'boro']
    gravitybk.loc[i,'NAME']=locdetail.loc[i,'name']
    gravitybk.loc[i,'ROUTES']=locdetail.loc[i,'routes']
    gravitybk.loc[i,'INTLAT']=locdetail.loc[i,'intlat']
    gravitybk.loc[i,'INTLONG']=locdetail.loc[i,'intlong']
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
gravitybk.to_csv(path+'allstation/outbound/allstationoutboundwtgravity.csv',index=True)


# wtprgravity
gvbk=pd.read_csv(path+'allstation/outbound/wtprbk.csv',dtype=str)
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
gvbk.to_csv(path+'allstation/outbound/allstationoutboundwtprgravity.csv',index=True,na_rep='0')


gvbk=pd.read_csv(path+'allstation/outbound/allstationoutboundwtprgravity.csv',dtype=str)
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


locdetail=pd.read_excel(path+'allstation/inbound/location.xlsx',sheet_name='location',dtype=str)
locdetail=locdetail.set_index('newlocationid')
locdetail=locdetail[['type','boro','name','routes','intlat','intlong']]

gravitybk=pd.DataFrame(index=loclist,columns=['TYPE','BORO','NAME','ROUTES','INTLAT','INTLONG',
                                              'WAC1-10','WAC11-20','WAC21-30','WAC31-40','WAC41-50','WAC51-60',
                                              'WG1-10','WG11-20','WG21-30','WG31-40','WG41-50','WG51-60','WGRAVITY'])
for i in loclist:
    gravitybk.loc[i,'TYPE']=locdetail.loc[i,'type']
    gravitybk.loc[i,'BORO']=locdetail.loc[i,'boro']
    gravitybk.loc[i,'NAME']=locdetail.loc[i,'name']
    gravitybk.loc[i,'ROUTES']=locdetail.loc[i,'routes']
    gravitybk.loc[i,'INTLAT']=locdetail.loc[i,'intlat']
    gravitybk.loc[i,'INTLONG']=locdetail.loc[i,'intlong']
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
gravitybk.to_csv(path+'allstation/outbound/allstationoutboundwtprgravity.csv',index=True)
