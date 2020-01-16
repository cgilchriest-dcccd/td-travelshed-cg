import geopandas as gpd
import numpy as np
import pandas as pd

nyc=['36005','36047','36061','36081','36085']

path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2018/TRAVELSHEDREVAMP/'

gdf=gpd.read_file(path+'shp/quadstatectclipped.shp')
gdf['tractid']=['RES'+str(x) for x in gdf['tractid']]
gdf=gdf[['tractid','geometry']].reset_index(drop=True)
gdf.to_file(path+'tableau/quadstatectclipped.shp')

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
df.to_csv(path+'tableau/resct.csv',index=False)



