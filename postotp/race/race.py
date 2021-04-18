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

# race
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


# income
income=pd.read_csv(path+'race/income.csv')
income['tractid']=[x[3:] for x in income['CT']]
df=pd.read_csv(path+'nyctract/resbkgravity3.csv')
df['tractid']=[x[3:] for x in df['Unnamed: 0']]
income=pd.merge(income,df,how='inner',on='tractid')
quadstatectclipped=gpd.read_file(path+'shp/quadstatectclipped.shp')
quadstatectclipped.crs=4326
income=pd.merge(quadstatectclipped,income,how='inner',on='tractid')

income=income[['tractid','TOTAL','INC01','INC02','INC03','INC04','INC05','INC06','INC07','INC08','INC09','INC10','INC11','GRAVITYWAC','geometry']].reset_index(drop=True)
income.to_file(path+'race/income.shp')
income['JOBSTD']=(income['GRAVITYWAC']-np.mean(income['GRAVITYWAC']))/np.std(income['GRAVITYWAC'])
income['JOBCAT']=np.where(income['JOBSTD']>=2,'>=+2SD',
                 np.where(income['JOBSTD']>=1.5,'+1.5SD ~ +2SD',
                 np.where(income['JOBSTD']>=1,'+1SD ~ +1.5SD',
                 np.where(income['JOBSTD']>=0.5,'+0.5SD ~ +1SD',
                 np.where(income['JOBSTD']>=0,'0SD ~ +0.5SD',
                 np.where(income['JOBSTD']>=-0.5,'-0.5SD ~ 0SD','<-0.5SD'))))))
income['JOBPCT']=pd.qcut(income['GRAVITYWAC'],100,labels=False)+1
income.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-travelshed/postotp/race/income.geojson',driver='GeoJSON')














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
df['OTHERA']=(df['NATIVEG']+df['PACIFICG']+df['OTHER1G']+df['OTHER2G'])/(df['NATIVE']+df['PACIFIC']+df['OTHER1']+df['OTHER2'])

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
df['OTHERA']=(df['NATIVEG']+df['PACIFICG']+df['OTHER1G']+df['OTHER2G'])/(df['NATIVE']+df['PACIFIC']+df['OTHER1']+df['OTHER2'])






df=pd.read_csv(path+'nyctract/resbkgravity3.csv')
df=df[['Unnamed: 0','GRAVITYWAC']].reset_index(drop=True)
df.columns=['CT','GRAVITYWAC']
inc=pd.read_csv(path+'race/income.csv')
df=pd.merge(df,inc,how='inner',on='CT')
df['COUNTY']=[x[3:8] for x in df['CT']]
df['TOTALG']=df['TOTAL']*df['GRAVITYWAC']
df['INC01G']=df['INC01']*df['GRAVITYWAC']
df['INC02G']=df['INC02']*df['GRAVITYWAC']
df['INC03G']=df['INC03']*df['GRAVITYWAC']
df['INC04G']=df['INC04']*df['GRAVITYWAC']
df['INC05G']=df['INC05']*df['GRAVITYWAC']
df['INC06G']=df['INC06']*df['GRAVITYWAC']
df['INC07G']=df['INC07']*df['GRAVITYWAC']
df['INC08G']=df['INC08']*df['GRAVITYWAC']
df['INC09G']=df['INC09']*df['GRAVITYWAC']
df['INC10G']=df['INC10']*df['GRAVITYWAC']
df['INC11G']=df['INC11']*df['GRAVITYWAC']
df=df.groupby(['COUNTY'],as_index=False).agg({'TOTALG':'sum','TOTAL':'sum','INC01G':'sum','INC01':'sum',
                                              'INC02G':'sum','INC02':'sum','INC03G':'sum','INC03':'sum',
                                              'INC04G':'sum','INC04':'sum','INC05G':'sum','INC05':'sum',
                                              'INC06G':'sum','INC06':'sum','INC07G':'sum','INC07':'sum',
                                              'INC08G':'sum','INC08':'sum','INC09G':'sum','INC09':'sum',
                                              'INC10G':'sum','INC10':'sum','INC11G':'sum','INC11':'sum'}).reset_index(drop=True)
df['TOTALA']=df['TOTALG']/df['TOTAL']
df['INC01A']=df['INC01G']/df['INC01']
df['INC02A']=df['INC02G']/df['INC02']
df['INC03A']=df['INC03G']/df['INC03']
df['INC04A']=df['INC04G']/df['INC04']
df['INC05A']=df['INC05G']/df['INC05']
df['INC06A']=df['INC06G']/df['INC06']
df['INC07A']=df['INC07G']/df['INC07']
df['INC08A']=df['INC08G']/df['INC08']
df['INC09A']=df['INC09G']/df['INC09']
df['INC10A']=df['INC10G']/df['INC10']
df['INC11A']=df['INC11G']/df['INC11']
df['INC01CBNA']=(df['INC01G']+df['INC02G']+df['INC03G']+df['INC04G']+df['INC05G'])/(df['INC01']+df['INC02']+df['INC03']+df['INC04']+df['INC05'])
df['INC02CBNA']=(df['INC06G']+df['INC07G'])/(df['INC06']+df['INC07'])
df['INC03CBNA']=df['INC08G']/df['INC08']
df['INC04CBNA']=df['INC09G']/df['INC09']
df['INC05CBNA']=df['INC10G']/df['INC10']
df['INC06CBNA']=df['INC11G']/df['INC11']

df['TT']=1
df=df.groupby(['TT'],as_index=False).agg({'TOTALG':'sum','TOTAL':'sum','INC01G':'sum','INC01':'sum',
                                          'INC02G':'sum','INC02':'sum','INC03G':'sum','INC03':'sum',
                                          'INC04G':'sum','INC04':'sum','INC05G':'sum','INC05':'sum',
                                          'INC06G':'sum','INC06':'sum','INC07G':'sum','INC07':'sum',
                                          'INC08G':'sum','INC08':'sum','INC09G':'sum','INC09':'sum',
                                          'INC10G':'sum','INC10':'sum','INC11G':'sum','INC11':'sum'}).reset_index(drop=True)
df['TOTALA']=df['TOTALG']/df['TOTAL']
df['INC01A']=df['INC01G']/df['INC01']
df['INC02A']=df['INC02G']/df['INC02']
df['INC03A']=df['INC03G']/df['INC03']
df['INC04A']=df['INC04G']/df['INC04']
df['INC05A']=df['INC05G']/df['INC05']
df['INC06A']=df['INC06G']/df['INC06']
df['INC07A']=df['INC07G']/df['INC07']
df['INC08A']=df['INC08G']/df['INC08']
df['INC09A']=df['INC09G']/df['INC09']
df['INC10A']=df['INC10G']/df['INC10']
df['INC11A']=df['INC11G']/df['INC11']
df['INC01CBNA']=(df['INC01G']+df['INC02G']+df['INC03G']+df['INC04G']+df['INC05G'])/(df['INC01']+df['INC02']+df['INC03']+df['INC04']+df['INC05'])
df['INC02CBNA']=(df['INC06G']+df['INC07G'])/(df['INC06']+df['INC07'])
df['INC03CBNA']=df['INC08G']/df['INC08']
df['INC04CBNA']=df['INC09G']/df['INC09']
df['INC05CBNA']=df['INC10G']/df['INC10']
df['INC06CBNA']=df['INC11G']/df['INC11']






