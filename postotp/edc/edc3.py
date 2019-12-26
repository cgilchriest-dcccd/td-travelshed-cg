#! /usr/bin/python3


import geopandas as gpd
import pandas as pd
import numpy as np
import dask.dataframe as dd

pd.set_option('display.max_columns', 500)
pd.set_option('display.max_colwidth',500)
pd.set_option('display.max_rows',500)

#path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2018/TRAVELSHEDREVAMP/'
#path='/home/mayijun/TRAVELSHED/'
path='E:/TRAVELSHEDREVAMP/'


# tract
# allstation
allstationlist=pd.read_excel(path+'edc/edc3/location3.xlsx',sheet_name='allstation',dtype=str)
df=dd.read_csv(path+'edc/allstationworkwtct.csv',dtype=str)
allstation=df[['tractid']+list(allstationlist['siteid'])]

# nyctract
nyctractlist=pd.read_excel(path+'edc/edc3/location3.xlsx',sheet_name='nyctract',dtype=str)
df=dd.read_csv(path+'edc/nyctractworkct.csv',dtype=str)
nyctract=df[['tractid']+list(['WORK36047'+x[1:7] for x in nyctractlist['siteid']])]
nyctract.columns=['tractid']+list(nyctractlist['siteid'])

# regionalrail
regionalraillist=pd.read_excel(path+'edc/edc3/location3.xlsx',sheet_name='regionalrail',dtype=str)
df=dd.read_csv(path+'edc/regionalworkwtct.csv',dtype=str)
regionalrail=df[['tractid']+list(regionalraillist['siteid'])]

# edc
edc=allstation.merge(regionalrail,on='tractid')
edc=edc.merge(nyctract,on='tractid')
edc=edc.compute()
for i in edc.columns[1:]:
    edc[i]=pd.to_numeric(edc[i])
wtct=gpd.read_file(path+'shp/quadstatectclipped.shp')
wtct.crs={'init': 'epsg:4326'}
wtct=wtct[['tractid','geometry']]
wtct=wtct.merge(edc,on='tractid')
wtct.to_file(path+'edc/edc3/tract.shp')
wtct=wtct.drop('geometry',axis=1)
wtct.to_csv(path+'edc/edc3/tract.csv',index=False)








# block
# allstation
allstationlist=pd.read_excel(path+'edc/edc3/location3.xlsx',sheet_name='allstation',dtype=str)
df=dd.read_csv(path+'edc/allstationworkwtbk.csv',dtype=str)
allstation=df[['blockid']+list(allstationlist['siteid'])]

# nyctract
nyctractlist=pd.read_excel(path+'edc/edc3/location3.xlsx',sheet_name='nyctract',dtype=str)
df=dd.read_csv(path+'edc/nyctractworkbk.csv',dtype=str)
nyctract=df[['blockid']+list(['WORK36047'+x[1:7] for x in nyctractlist['siteid']])]
nyctract.columns=['blockid']+list(nyctractlist['siteid'])

# regionalrail
regionalraillist=pd.read_excel(path+'edc/edc3/location3.xlsx',sheet_name='regionalrail',dtype=str)
df=dd.read_csv(path+'edc/regionalworkwtbk.csv',dtype=str)
regionalrail=df[['blockid']+list(regionalraillist['siteid'])]

# edc
edc=allstation.merge(regionalrail,on='blockid')
edc=edc.merge(nyctract,on='blockid')
edc=edc.compute()
for i in edc.columns[1:]:
    edc[i]=pd.to_numeric(edc[i])
edc.to_csv(path+'edc/edc3/block.csv',index=False)
wtbk=gpd.read_file(path+'shp/quadstatebkclipped.shp')
wtbk.crs={'init': 'epsg:4326'}
wtbk=wtbk[['blockid','geometry']]
wtbk=wtbk.merge(edc,on='blockid')
wtbk.to_file(path+'edc/edc3/block.shp')

# gravity
gvbk=pd.read_csv(path+'edc/edc3/block.csv',dtype=str)
gvbk=gvbk.set_index('blockid')
loclist=gvbk.columns
rac=pd.DataFrame()
for i in ['ct','nj','ny','pa']:
    tp=pd.read_csv(path+'lehd/'+str(i)+'_rac_S000_JT01_2015.csv',dtype=str)
    tp=tp[['h_geocode','C000']]
    rac=pd.concat([rac,tp],axis=0)
rac.columns=['blockid','rac']
rac=rac.set_index('blockid')
gvbk=pd.merge(gvbk,rac,how='left',left_index=True,right_index=True)
gvbk.to_csv(path+'edc/edc3/blockgravity.csv',index=True,na_rep='0')
gvbk=pd.read_csv(path+'edc/edc3/blockgravity.csv',dtype=str)
gvbk=gvbk.set_index('blockid')
loclist=gvbk.columns[0:-1]
for i in loclist:
    gvbk[i]=pd.to_numeric(gvbk[i])
    gvbk[i]=np.where(gvbk[i]<=5,2.5,
            np.where(gvbk[i]<=10,7.5,
            np.where(gvbk[i]<=15,12.5,
            np.where(gvbk[i]<=20,17.5,
            np.where(gvbk[i]<=25,22.5,
            np.where(gvbk[i]<=30,27.5,
            np.where(gvbk[i]<=35,32.5,
            np.where(gvbk[i]<=40,37.5,
            np.where(gvbk[i]<=45,42.5,
            np.where(gvbk[i]<=50,47.5,
            np.where(gvbk[i]<=55,52.5,
            np.where(gvbk[i]<=60,57.5,
            np.where(gvbk[i]<=65,62.5,
            np.where(gvbk[i]<=70,67.5,
            np.where(gvbk[i]<=75,72.5,
            np.nan)))))))))))))))
gvbk['rac']=pd.to_numeric(gvbk['rac'])
locdetail=pd.read_excel(path+'edc/edc3/location3.xlsx',sheet_name='summary',dtype=str)
locdetail=locdetail.set_index('siteid')
locdetail=locdetail[['type','boro','name','routes','lat','long']]
gravitybk=pd.DataFrame(index=loclist,columns=['TYPE','BORO','NAME','ROUTES','LAT','LONG',
                                              'LF1-5','LF6-10','LF11-15','LF16-20','LF21-25','LF26-30',
                                              'LF31-35','LF36-40','LF41-45','LF46-50','LF51-55','LF56-60',
                                              'LF61-65','LF66-70','LF71-75'])
for i in loclist:
    gravitybk.loc[i,'TYPE']=locdetail.loc[i,'type']
    gravitybk.loc[i,'BORO']=locdetail.loc[i,'boro']
    gravitybk.loc[i,'NAME']=locdetail.loc[i,'name']
    gravitybk.loc[i,'ROUTES']=locdetail.loc[i,'routes']
    gravitybk.loc[i,'LAT']=locdetail.loc[i,'lat']
    gravitybk.loc[i,'LONG']=locdetail.loc[i,'long']
    tp=sum(gvbk.loc[gvbk[i]==2.5,'rac'])
    gravitybk.loc[i,'LF1-5']=tp
    tp=sum(gvbk.loc[gvbk[i]==7.5,'rac'])
    gravitybk.loc[i,'LF6-10']=tp
    tp=sum(gvbk.loc[gvbk[i]==12.5,'rac'])
    gravitybk.loc[i,'LF11-15']=tp
    tp=sum(gvbk.loc[gvbk[i]==17.5,'rac'])
    gravitybk.loc[i,'LF16-20']=tp
    tp=sum(gvbk.loc[gvbk[i]==22.5,'rac'])
    gravitybk.loc[i,'LF21-25']=tp
    tp=sum(gvbk.loc[gvbk[i]==27.5,'rac'])
    gravitybk.loc[i,'LF26-30']=tp
    tp=sum(gvbk.loc[gvbk[i]==32.5,'rac'])
    gravitybk.loc[i,'LF31-35']=tp
    tp=sum(gvbk.loc[gvbk[i]==37.5,'rac'])
    gravitybk.loc[i,'LF36-40']=tp
    tp=sum(gvbk.loc[gvbk[i]==42.5,'rac'])
    gravitybk.loc[i,'LF41-45']=tp
    tp=sum(gvbk.loc[gvbk[i]==47.5,'rac'])
    gravitybk.loc[i,'LF46-50']=tp
    tp=sum(gvbk.loc[gvbk[i]==52.5,'rac'])
    gravitybk.loc[i,'LF51-55']=tp
    tp=sum(gvbk.loc[gvbk[i]==57.5,'rac'])
    gravitybk.loc[i,'LF56-60']=tp
    tp=sum(gvbk.loc[gvbk[i]==62.5,'rac'])
    gravitybk.loc[i,'LF61-65']=tp
    tp=sum(gvbk.loc[gvbk[i]==67.5,'rac'])
    gravitybk.loc[i,'LF66-70']=tp
    tp=sum(gvbk.loc[gvbk[i]==72.5,'rac'])
    gravitybk.loc[i,'LF71-75']=tp    
gravitybk.to_csv(path+'edc/edc3/edc3.csv',index=True)












