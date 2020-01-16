import geopandas as gpd
import numpy as np
import pandas as pd

pd.set_option('display.max_columns', None)

path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2018/TRAVELSHEDREVAMP/'
path='E:/TRAVELSHEDREVAMP/'

nyc=['36005','36047','36061','36081','36085']

gdf=gpd.read_file(path+'shp/quadstatectclipped.shp')
gdf['tractid']=['RES'+str(x) for x in gdf['tractid']]
gdf=gdf[['tractid','geometry']].reset_index(drop=True)
gdf.to_file(path+'tableau/quadstatectclipped.shp')
gdf=gdf[[np.isin(str(x)[3:8],nyc) for x in gdf['tractid']]]
gdf.to_file(path+'tableau/nyctract.shp')

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
df['wac']=np.where(df['time']<=10,'WAC1-10',
          np.where(df['time']<=20,'WAC11-20',
          np.where(df['time']<=30,'WAC21-30',
          np.where(df['time']<=40,'WAC31-40',
          np.where(df['time']<=50,'WAC41-50',
          np.where(df['time']<=60,'WAC51-60',''))))))
df.to_csv(path+'tableau/resct.csv',index=False)

resbkgravity=pd.read_csv(path+'nyctract/resbkgravity3.csv',dtype=float,converters={'Unnamed: 0':str})
resbkgravity['WAC1-10']=resbkgravity['WAC1-5']+resbkgravity['WAC6-10']
resbkgravity['WAC11-20']=resbkgravity['WAC11-15']+resbkgravity['WAC16-20']
resbkgravity['WAC21-30']=resbkgravity['WAC21-25']+resbkgravity['WAC26-30']
resbkgravity['WAC31-40']=resbkgravity['WAC31-35']+resbkgravity['WAC36-40']
resbkgravity['WAC41-50']=resbkgravity['WAC41-45']+resbkgravity['WAC46-50']
resbkgravity['WAC51-60']=resbkgravity['WAC51-55']+resbkgravity['WAC56-60']
resbkgravity=resbkgravity[['Unnamed: 0','WAC1-10','WAC11-20','WAC21-30','WAC31-40','WAC41-50','WAC51-60']].reset_index(drop=True)
resbkgravity=resbkgravity.rename(columns={'Unnamed: 0':'restractid'}).reset_index(drop=True)
resbkgravity=resbkgravity.melt(id_vars=['restractid'],var_name='cat',value_name='wac').reset_index(drop=True)
resbkgravity.to_csv(path+'tableau/resbkgravity.csv',index=False)
