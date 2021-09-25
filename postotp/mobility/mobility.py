import pandas as pd
import numpy as np
import geopandas as gpd



pd.set_option('display.max_columns', None)
path='C:/Users/mayij/Desktop/DOC/DCP2018/TRAVELSHEDREVAMP/'




# From
hd=pd.DataFrame(columns=['orgct','destbk','time'])
hd.to_csv(path+'mobility/from.csv',mode='w',index=False,header=True)
rd=pd.read_csv(path+'nyctract/resbk3.csv',dtype=float,converters={'blockid':str},chunksize=10000)
for ck in rd:
    tp=pd.melt(ck,id_vars=['blockid'])
    tp=tp[tp['value']<=45].reset_index(drop=True)
    tp['orgct']=[x.replace('RES','') for x in tp['variable']]
    tp['destbk']=tp['blockid'].copy()
    tp['time']=tp['value'].copy()
    tp=tp[['orgct','destbk','time']].reset_index(drop=True)
    tp.to_csv(path+'mobility/from.csv',mode='a',index=False,header=False)

df=pd.read_csv(path+'mobility/from.csv',dtype=str,converters={'time':float})
bk=gpd.read_file(path+'shp/quadstatebkclipped.shp')
bk.crs=4326
bk=bk.to_crs(6539)
bk['area']=bk.area
bk['acre']=bk['area']/43560
bk=bk[['blockid','acre']].reset_index(drop=True)
df=pd.merge(df,bk,how='inner',left_on='destbk',right_on='blockid')
df=df.groupby(['orgct'],as_index=False).agg({'acre':'sum'}).reset_index(drop=True)
df.columns=['tractid','acre45']
df['std']=(df['acre45']-np.mean(df['acre45']))/np.std(df['acre45'])
df['stdcat']=np.where(df['std']>=2.5,'>=+2.5SD',
             np.where(df['std']>=1.5,'+1.5SD ~ +2.5SD',
             np.where(df['std']>=0.5,'+0.5SD ~ +1.5SD',
             np.where(df['std']>=-0.5,'-0.5SD ~ +0.5SD',
             np.where(df['std']>=-1.5,'-1.5SD ~ -0.5SD',
             np.where(df['std']>=-2.5,'-2.5SD ~ -1.5SD','<-2.5SD'))))))
df['pct']=pd.qcut(df['acre45'],100,labels=False)
df=df[['tractid','acre45','std','stdcat','pct']].reset_index(drop=True)

ct=gpd.read_file(path+'shp/quadstatectclipped.shp')
ct.crs=4326
ct=ct[['tractid','geometry']].reset_index(drop=True)
df=pd.merge(ct,df,how='inner',on='tractid')
df.to_file(path+'mobility/from.shp')






# To
hd=pd.DataFrame(columns=['destct','orgbk','time'])
hd.to_csv(path+'mobility/to.csv',mode='w',index=False,header=True)
rd=pd.read_csv(path+'nyctract/workbk3.csv',dtype=float,converters={'blockid':str},chunksize=10000)
for ck in rd:
    tp=pd.melt(ck,id_vars=['blockid'])
    tp=tp[tp['value']<=45].reset_index(drop=True)
    tp['destct']=[x.replace('WORK','') for x in tp['variable']]
    tp['orgbk']=tp['blockid'].copy()
    tp['time']=tp['value'].copy()
    tp=tp[['destct','orgbk','time']].reset_index(drop=True)
    tp.to_csv(path+'mobility/to.csv',mode='a',index=False,header=False)

df=pd.read_csv(path+'mobility/to.csv',dtype=str,converters={'time':float})
bk=gpd.read_file(path+'shp/quadstatebkclipped.shp')
bk.crs=4326
bk=bk.to_crs(6539)
bk['area']=bk.area
bk['acre']=bk['area']/43560
bk=bk[['blockid','acre']].reset_index(drop=True)
df=pd.merge(df,bk,how='inner',left_on='orgbk',right_on='blockid')
df=df.groupby(['destct'],as_index=False).agg({'acre':'sum'}).reset_index(drop=True)
df.columns=['tractid','acre45']
df['std']=(df['acre45']-np.mean(df['acre45']))/np.std(df['acre45'])
df['stdcat']=np.where(df['std']>=2.5,'>=+2.5SD',
             np.where(df['std']>=1.5,'+1.5SD ~ +2.5SD',
             np.where(df['std']>=0.5,'+0.5SD ~ +1.5SD',
             np.where(df['std']>=-0.5,'-0.5SD ~ +0.5SD',
             np.where(df['std']>=-1.5,'-1.5SD ~ -0.5SD',
             np.where(df['std']>=-2.5,'-2.5SD ~ -1.5SD','<-2.5SD'))))))
df['pct']=pd.qcut(df['acre45'],100,labels=False)
df=df[['tractid','acre45','std','stdcat','pct']].reset_index(drop=True)

ct=gpd.read_file(path+'shp/quadstatectclipped.shp')
ct.crs=4326
ct=ct[['tractid','geometry']].reset_index(drop=True)
df=pd.merge(ct,df,how='inner',on='tractid')
df.to_file(path+'mobility/to.shp')
