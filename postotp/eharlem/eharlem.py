import datetime
import geopandas as gpd
import pandas as pd
import numpy as np

start=datetime.datetime.now()

pd.set_option('display.max_columns', None)
path='E:/TRAVELSHEDREVAMP/'

ctpt=gpd.read_file(path+'shp/quadstatectpt.shp')
ctpt.crs={'init': 'epsg:4326'}
ctpt=ctpt[['tractid','geometry']].reset_index(drop=True)
cdwater=gpd.read_file(path+'eharlem/cdwater.shp')
cdwater.crs={'init': 'epsg:4326'}
cd=gpd.read_file(path+'eharlem/cd.shp')
cd.crs={'init': 'epsg:4326'}

ctptcdwater=gpd.sjoin(ctpt,cdwater,how='inner',op='intersects')
ctptcdwater=ctptcdwater[['tractid','BoroCD']].reset_index(drop=True)

lehd=pd.read_csv(path+'lehd/ny_rac_S000_JT03_2017.csv',dtype=str)
lehd['tractid']=[str(x)[0:11] for x in lehd['h_geocode']]
lehd['rac']=pd.to_numeric(lehd['C000'])
lehd=lehd[['tractid','rac']].reset_index(drop=True)
lehd=lehd.groupby('tractid',as_index=False).agg({'rac':'sum'}).reset_index(drop=True)

resbkgravity=pd.read_csv(path+'eharlem/resbkgravity3.csv',dtype=float,converters={'Unnamed: 0':str})
resbkgravity['tractid']=[str(x)[3:15] for x in resbkgravity['Unnamed: 0']]
resbkgravity=resbkgravity[['tractid','GRAVITYWAC']].reset_index(drop=True)
resbkgravity=pd.merge(resbkgravity,ctptcdwater,how='inner',on='tractid')
resbkgravity=pd.merge(resbkgravity,lehd,how='left',on='tractid')
resbkgravity['rac']=resbkgravity['rac'].replace(np.nan,0)
rescdgravity=resbkgravity.groupby('BoroCD',as_index=True).apply(lambda x:np.average(x['GRAVITYWAC'],weights=x['rac'])).reset_index(drop=False)
rescdgravity.columns=['BoroCD','GRAVITYWAC']
rescdgravity=pd.merge(cd,rescdgravity,how='left',on='BoroCD')
rescdgravity.to_file(path+'eharlem/rescdgravity.shp')
rescdgravity=rescdgravity.drop('geometry',axis=1).reset_index(drop=True)
rescdgravity.to_csv(path+'eharlem/rescdgravity.csv',index=False)
