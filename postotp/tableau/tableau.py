import geopandas as gpd
import numpy as np
import pandas as pd

pd.set_option('display.max_columns', None)

path='E:/TRAVELSHEDREVAMP/'
path='C:/Users/mayij/Desktop/DOC/DCP2018/TRAVELSHEDREVAMP/'

nyc=['36005','36047','36061','36081','36085']

gdf=gpd.read_file(path+'shp/quadstatectclipped.shp')
gdf=gdf[['tractid','geometry']].reset_index(drop=True)
gdf.to_file(path+'tableau/quadstatectclipped.shp')
gdf=gdf[[np.isin(str(x)[0:5],nyc) for x in gdf['tractid']]]
gdf.to_file(path+'tableau/nyctract.shp')

resct=pd.read_csv(path+'nyctract/resct3.csv',dtype=float,converters={'tractid':str})
df=pd.DataFrame()
for i in resct.columns[1:]:
    tp=resct[['tractid',i]].reset_index(drop=True)
    tp.columns=['worktractid','time']
    tp['restractid']=str(i)[3:14]
    tp=tp.loc[tp['time']<=60,['restractid','worktractid','time']].reset_index(drop=True)
    df=pd.concat([df,tp],axis=0,ignore_index=True)
df['cat']=np.where(df['time']<=10,'WAC1-10',
          np.where(df['time']<=20,'WAC11-20',
          np.where(df['time']<=30,'WAC21-30',
          np.where(df['time']<=40,'WAC31-40',
          np.where(df['time']<=50,'WAC41-50',
          np.where(df['time']<=60,'WAC51-60',''))))))
df.to_csv(path+'tableau/resct.csv',index=False)

resbkgravity=pd.read_csv(path+'nyctract/resbkgravity3.csv',dtype=float,converters={'Unnamed: 0':str})
resbkgravity['restractid']=[str(x)[3:14] for x in resbkgravity['Unnamed: 0']]
resbkgravity['WAC1-10']=resbkgravity['WAC1-5']+resbkgravity['WAC6-10']
resbkgravity['WAC11-20']=resbkgravity['WAC11-15']+resbkgravity['WAC16-20']
resbkgravity['WAC21-30']=resbkgravity['WAC21-25']+resbkgravity['WAC26-30']
resbkgravity['WAC31-40']=resbkgravity['WAC31-35']+resbkgravity['WAC36-40']
resbkgravity['WAC41-50']=resbkgravity['WAC41-45']+resbkgravity['WAC46-50']
resbkgravity['WAC51-60']=resbkgravity['WAC51-55']+resbkgravity['WAC56-60']
resbkgravity=resbkgravity[['restractid','WAC1-10','WAC11-20','WAC21-30','WAC31-40','WAC41-50','WAC51-60']].reset_index(drop=True)
resbkgravity=resbkgravity.melt(id_vars=['restractid'],var_name='cat',value_name='wac').reset_index(drop=True)
resbkgravity.to_csv(path+'tableau/resbkgravity.csv',index=False)

workct=pd.read_csv(path+'nyctract/workct3.csv',dtype=float,converters={'tractid':str})
df=pd.DataFrame()
for i in workct.columns[1:]:
    tp=workct[['tractid',i]].reset_index(drop=True)
    tp.columns=['restractid','time']
    tp['worktractid']=str(i)[4:15]
    tp=tp.loc[tp['time']<=60,['worktractid','restractid','time']].reset_index(drop=True)
    df=pd.concat([df,tp],axis=0,ignore_index=True)
df['cat']=np.where(df['time']<=10,'RAC1-10',
          np.where(df['time']<=20,'RAC11-20',
          np.where(df['time']<=30,'RAC21-30',
          np.where(df['time']<=40,'RAC31-40',
          np.where(df['time']<=50,'RAC41-50',
          np.where(df['time']<=60,'RAC51-60',''))))))
df.to_csv(path+'tableau/workct.csv',index=False)

workbkgravity=pd.read_csv(path+'nyctract/workbkgravity3.csv',dtype=float,converters={'Unnamed: 0':str})
workbkgravity['worktractid']=[str(x)[4:15] for x in workbkgravity['Unnamed: 0']]
workbkgravity['RAC1-10']=workbkgravity['RAC1-5']+workbkgravity['RAC6-10']
workbkgravity['RAC11-20']=workbkgravity['RAC11-15']+workbkgravity['RAC16-20']
workbkgravity['RAC21-30']=workbkgravity['RAC21-25']+workbkgravity['RAC26-30']
workbkgravity['RAC31-40']=workbkgravity['RAC31-35']+workbkgravity['RAC36-40']
workbkgravity['RAC41-50']=workbkgravity['RAC41-45']+workbkgravity['RAC46-50']
workbkgravity['RAC51-60']=workbkgravity['RAC51-55']+workbkgravity['RAC56-60']
workbkgravity=workbkgravity[['worktractid','RAC1-10','RAC11-20','RAC21-30','RAC31-40','RAC41-50','RAC51-60']].reset_index(drop=True)
workbkgravity=workbkgravity.melt(id_vars=['worktractid'],var_name='cat',value_name='rac').reset_index(drop=True)
workbkgravity.to_csv(path+'tableau/workbkgravity.csv',index=False)
