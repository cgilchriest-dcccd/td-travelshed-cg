import geopandas as gpd
import numpy as np
import pandas as pd

nyc=['36005','36047','36061','36081','36085']

path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2018/TRAVELSHEDREVAMP/'

gdf=gpd.read_file(path+'shp/quadstatectclipped.shp')
gdf['restractid']=['RES'+str(x) for x in gdf['tractid']]
gdf=gdf[[np.isin(str(x)[3:8],nyc) for x in gdf['restractid']]].reset_index(drop=True)
gdf=gdf[['restractid','tractid','geometry']].reset_index(drop=True)
gdf.to_file('C:/Users/Yijun Ma/Desktop/nyctract.shp')


gdf=gpd.read_file(path+'shp/quadstatectclipped.shp')
gdf=gdf[['tractid','geometry']].reset_index(drop=True)

resct=pd.read_csv(path+'nyctract/resct3.csv',dtype=float,converters={'tractid':str})

df=pd.DataFrame()
for i in resct.columns[1:]:
    tp=resct[['tractid',i]].reset_index(drop=True)
    tp.columns=['tractid','time']
    tp['restractid']=i
    tp=tp.loc[tp['time']<=60,['restractid','tractid','time']].reset_index(drop=True)
    df=pd.concat([df,tp],axis=0,ignore_index=True)
df=pd.merge(gdf,df,how='right',on='tractid').reset_index(drop=True)
df=df[['restractid','tractid','time','geometry']].reset_index(drop=True)
df.to_file('C:/Users/Yijun Ma/Desktop/resct.shp')






gdf=gpd.read_file(path+'shp/quadstatectclipped.shp')
gdf['tractid']=['RES'+str(x) for x in gdf['tractid']]
gdf=gdf[['tractid','geometry']].reset_index(drop=True)
gdf.to_file('C:/Users/Yijun Ma/Desktop/quadstatectclipped.shp')

resct=pd.read_csv(path+'nyctract/resct3.csv',dtype=float,converters={'tractid':str})
df=pd.DataFrame()
for i in resct.columns[1:]:
    tp=resct[['tractid',i]].reset_index(drop=True)
    tp.columns=['tractid','time']
    tp['restractid']=i
    tp=tp.loc[tp['time']<=60,['restractid','tractid','time']].reset_index(drop=True)
    df=pd.concat([df,tp],axis=0,ignore_index=True)
df['tractid']=['RES'+str(x) for x in df['tractid']]
df=df[['restractid','tractid','time']].reset_index(drop=True)
df.to_csv('C:/Users/Yijun Ma/Desktop/resct.csv',index=False)



