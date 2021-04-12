import geopandas as gpd
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
import plotly.io as pio
import json

pd.set_option('display.max_columns', None)
path='C:/Users/mayij/Desktop/DOC/DCP2018/TRAVELSHEDREVAMP/'
pio.renderers.default = "browser"


race=pd.read_csv(path+'race/race.csv')
race['tractid']=[x[3:] for x in race['CT']]
df=pd.read_csv(path+'nyctract/resbkgravity3.csv')
df['tractid']=[x[3:] for x in df['Unnamed: 0']]
race=pd.merge(race,df,how='inner',on='tractid')
quadstatectclipped=gpd.read_file(path+'shp/quadstatectclipped.shp')
quadstatectclipped.crs=4326
race=pd.merge(quadstatectclipped,race,how='inner',on='tractid')
race=race[['tractid','TOTAL','WHITE','BLACK','NATIVE','ASIAN','PACIFIC','OTHER1','OTHER2','GRAVITYWAC','geometry']].reset_index(drop=True)
race.to_file(path+'race/race.shp')
race['JOBSTD']=(race['GRAVITYWAC']-np.mean(race['GRAVITYWAC']))/np.std(race['GRAVITYWAC'])
race['JOBCAT']=np.where(race['JOBSTD']>=2,'>=+2SD',
               np.where(race['JOBSTD']>=1.5,'+1.5SD ~ +2SD',
               np.where(race['JOBSTD']>=1,'+1SD ~ +1.5SD',
               np.where(race['JOBSTD']>=0.5,'+0.5SD ~ +1SD',
               np.where(race['JOBSTD']>=0,'0SD ~ +0.5SD',
               np.where(race['JOBSTD']>=-0.5,'-0.5SD ~ 0SD','<-0.5SD'))))))
race['JOBPCT']=pd.qcut(race['GRAVITYWAC'],100,labels=False)+1
race.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-travelshed/postotp/race/race.geojson',driver='GeoJSON')






df=pd.read_csv(path+'nyctract/resbkgravity3.csv')
df=df[['Unnamed: 0','GRAVITYWAC']].reset_index(drop=True)
df.columns=['CT','GRAVITYWAC']
race=pd.read_csv(path+'race/race.csv')
df=pd.merge(df,race,how='inner',on='CT')
df['COUNTY']=[x[3:8] for x in df['CT']]
df['TOTALG']=df['TOTAL']*df['GRAVITYWAC']
df['WHITEG']=df['WHITE']*df['GRAVITYWAC']
df['BLACKG']=df['BLACK']*df['GRAVITYWAC']
df['NATIVEG']=df['NATIVE']*df['GRAVITYWAC']
df['ASIANG']=df['ASIAN']*df['GRAVITYWAC']
df['PACIFICG']=df['PACIFIC']*df['GRAVITYWAC']
df['OTHER1G']=df['OTHER1']*df['GRAVITYWAC']
df['OTHER2G']=df['OTHER2']*df['GRAVITYWAC']
df=df.groupby(['COUNTY'],as_index=False).agg({'TOTALG':'sum','TOTAL':'sum','WHITEG':'sum','WHITE':'sum',
                                              'BLACKG':'sum','BLACK':'sum','NATIVEG':'sum','NATIVE':'sum',
                                              'ASIANG':'sum','ASIAN':'sum','PACIFICG':'sum','PACIFIC':'sum',
                                              'OTHER1G':'sum','OTHER1':'sum','OTHER2G':'sum','OTHER2':'sum'}).reset_index(drop=True)
df['TOTALA']=df['TOTALG']/df['TOTAL']
df['WHITEA']=df['WHITEG']/df['WHITE']
df['BLACKA']=df['BLACKG']/df['BLACK']
df['NATIVEA']=df['NATIVEG']/df['NATIVE']
df['ASIANA']=df['ASIANG']/df['ASIAN']
df['PACIFICA']=df['PACIFICG']/df['PACIFIC']
df['OTHER1A']=df['OTHER1G']/df['OTHER1']
df['OTHER2A']=df['OTHER2G']/df['OTHER2']
df['TT']=1
df=df.groupby(['TT'],as_index=False).agg({'TOTALG':'sum','TOTAL':'sum','WHITEG':'sum','WHITE':'sum',
                                          'BLACKG':'sum','BLACK':'sum','NATIVEG':'sum','NATIVE':'sum',
                                          'ASIANG':'sum','ASIAN':'sum','PACIFICG':'sum','PACIFIC':'sum',
                                          'OTHER1G':'sum','OTHER1':'sum','OTHER2G':'sum','OTHER2':'sum'}).reset_index(drop=True)
df['TOTALA']=df['TOTALG']/df['TOTAL']
df['WHITEA']=df['WHITEG']/df['WHITE']
df['BLACKA']=df['BLACKG']/df['BLACK']
df['NATIVEA']=df['NATIVEG']/df['NATIVE']
df['ASIANA']=df['ASIANG']/df['ASIAN']
df['PACIFICA']=df['PACIFICG']/df['PACIFIC']
df['OTHER1A']=df['OTHER1G']/df['OTHER1']
df['OTHER2A']=df['OTHER2G']/df['OTHER2']








