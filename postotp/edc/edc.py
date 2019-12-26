#! /usr/bin/python3


import geopandas as gpd
import pandas as pd
import numpy as np


#path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2018/TRAVELSHEDREVAMP/'
#path='/home/mayijun/TRAVELSHED/'
path='E:/TRAVELSHEDREVAMP/'


# tract
# allstation
allstation=pd.read_excel(path+'edc/location2.xlsx',sheet_name='allstation',dtype=str)
df=pd.read_csv(path+'edc/allstationworkwtct.csv',dtype=str)
allstation=df[['tractid']+list(allstation['siteid'])]

# nyctract
nyctract=pd.read_excel(path+'edc/location2.xlsx',sheet_name='nyctract',dtype=str)
df=pd.read_csv(path+'edc/nyctractworkct.csv',dtype=str)
nyctract=df[['tractid']+list(nyctract['siteid'])]

# regionalrail
regionalrail=pd.read_excel(path+'edc/location2.xlsx',sheet_name='regionalrail',dtype=str)
df=pd.read_csv(path+'edc/regionalworkwtct.csv',dtype=str)
regionalrail=df[['tractid']+list(regionalrail['siteid'])]

# edc
edc=allstation.merge(regionalrail,on='tractid')
edc=edc.merge(nyctract,on='tractid')
for i in edc.columns[1:]:
    edc[i]=pd.to_numeric(edc[i])
wtct=gpd.read_file(path+'shp/quadstatectclipped.shp')
wtct.crs={'init': 'epsg:4326'}
wtct=wtct[['tractid','geometry']]
wtct=wtct.merge(edc,on='tractid')
wtct.to_file(path+'edc/tract.shp')
wtct=wtct.drop('geometry',axis=1)
wtct.to_csv(path+'edc/tract.csv',index=False)








# block
# allstation
allstation=pd.read_excel(path+'edc/location2.xlsx',sheet_name='allstation',dtype=str)
df=pd.read_csv(path+'edc/allstationworkwtbk.csv',dtype=str)
allstation=df[['blockid']+list(allstation['siteid'])]
allstation.to_csv(path+'edc/allstation.csv',index=False)

# nyctract
nyctract=pd.read_excel(path+'edc/location2.xlsx',sheet_name='nyctract',dtype=str)
df=pd.read_csv(path+'edc/nyctractworkbk.csv',dtype=str)
nyctract=df[['blockid']+list(nyctract['siteid'])]
nyctract.to_csv(path+'edc/nyctract.csv',index=False)

# regionalrail
regionalrail=pd.read_excel(path+'edc/location2.xlsx',sheet_name='regionalrail',dtype=str)
df=pd.read_csv(path+'edc/regionalworkwtbk.csv',dtype=str)
regionalrail=df[['blockid']+list(regionalrail['siteid'])]
regionalrail.to_csv(path+'edc/regionalrail.csv',index=False)

# edc
allstation=pd.read_csv(path+'edc/allstation.csv',dtype=str)
nyctract=pd.read_csv(path+'edc/nyctract.csv',dtype=str)
regionalrail=pd.read_csv(path+'edc/regionalrail.csv',dtype=str)
edc=allstation.merge(regionalrail,on='blockid')
edc=edc.merge(nyctract,on='blockid')
for i in edc.columns[1:]:
    edc[i]=pd.to_numeric(edc[i])
edc.to_csv(path+'edc/block.csv',index=False)
wtbk=gpd.read_file(path+'shp/quadstatebkclipped.shp')
wtbk.crs={'init': 'epsg:4326'}
wtbk=wtbk[['blockid','geometry']]
wtbk=wtbk.merge(edc,on='blockid')
wtbk.to_file(path+'edc/block.shp')

# gravity
gvbk=pd.read_csv(path+'edc/block.csv',dtype=str)
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
gvbk.to_csv(path+'edc/blockgravity.csv',index=True,na_rep='0')
gvbk=pd.read_csv(path+'edc/blockgravity.csv',dtype=str)
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
            np.nan))))))))))))
gvbk['rac']=pd.to_numeric(gvbk['rac'])
locdetail=pd.read_excel(path+'edc/location2.xlsx',sheet_name='summary',dtype=str)
locdetail=locdetail.set_index('siteid')
locdetail=locdetail[['type','boro','name','routes','lat','long','neighborhood']]
gravitybk=pd.DataFrame(index=loclist,columns=['TYPE','BORO','NAME','ROUTES','LAT','LONG','NEIGHBORHOOD',
                                              'LF1-5','LF6-10','LF11-15','LF16-20','LF21-25','LF26-30',
                                              'LF31-35','LF36-40','LF41-45','LF46-50','LF51-55','LF56-60'])
for i in loclist:
    gravitybk.loc[i,'TYPE']=locdetail.loc[i,'type']
    gravitybk.loc[i,'BORO']=locdetail.loc[i,'boro']
    gravitybk.loc[i,'NAME']=locdetail.loc[i,'name']
    gravitybk.loc[i,'ROUTES']=locdetail.loc[i,'routes']
    gravitybk.loc[i,'LAT']=locdetail.loc[i,'lat']
    gravitybk.loc[i,'LONG']=locdetail.loc[i,'long']
    gravitybk.loc[i,'NEIGHBORHOOD']=locdetail.loc[i,'neighborhood']
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
gravitybk.to_csv(path+'edc/edc.csv',index=True)












