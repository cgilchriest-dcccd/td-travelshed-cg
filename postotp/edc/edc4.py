import requests
import pandas as pd
import geopandas as gpd
import numpy as np
import shapely
from shapely import wkt

pd.set_option('display.max_columns', None)
path='C:/Users/mayij/Desktop/DOC/DCP2018/TRAVELSHEDREVAMP/'



# Reading files
stops=pd.read_csv(path+'stops.txt',dtype=str)
shapes=pd.read_csv(path+'shapes.txt',dtype=str)
stoptimes=pd.read_csv(path+'stop_times.txt',dtype=str)
trips=pd.read_csv(path+'trips.txt',dtype=str)
routes=pd.read_csv(path+'routes.txt',dtype=str)
stoptimes=stoptimes[['trip_id','stop_id']].drop_duplicates(keep='first').reset_index(drop=True)
trips=trips[['trip_id','route_id','shape_id','trip_headsign','direction_id']].drop_duplicates(keep='first').reset_index(drop=True)
# routes=routes[['route_id','route_short_name','route_long_name','route_desc']].drop_duplicates(keep='first').reset_index(drop=True)
routes=routes[['route_id','route_short_name','route_long_name']].drop_duplicates(keep='first').reset_index(drop=True)

# Stops
stops2=pd.merge(stops,stoptimes,how='left',on='stop_id')
stops2=pd.merge(stops2,trips,how='left',on='trip_id')
stops2=pd.merge(stops2,routes,how='left',on='route_id')
stops2=stops2.groupby(['stop_id','stop_name','stop_lat','stop_lon','route_short_name'],as_index=False).agg({'trip_id':'count'})
# stops2['stop_id']=[str(x)[:-1] for x in stops2['stop_id']]
stops2=stops2.sort_values(['stop_id','trip_id'],ascending=[True,False]).reset_index(drop=True)
stops2=stops2.drop(['trip_id'],axis=1).reset_index(drop=True)
stops2=stops2.drop_duplicates(keep='first').reset_index(drop=True)
stops2['sbs']=[x.find('-SBS') for x in stops2['route_short_name']]
stops2['exp']=[x.find('X') for x in stops2['route_short_name']]
stops2['sim']=[x.find('SIM') for x in stops2['route_short_name']]
stops2['bm']=[x.find('BM') for x in stops2['route_short_name']]
stops2['bxm']=[x.find('BxM') for x in stops2['route_short_name']]
stops2['qm']=[x.find('QM') for x in stops2['route_short_name']]
stops2=stops2[(stops2['sbs']!=-1)|(stops2['exp']!=-1)|(stops2['sim']!=-1)|(stops2['bm']!=-1)|(stops2['bxm']!=-1)|(stops2['qm']!=-1)].reset_index(drop=True)
stops2=stops2.groupby(['stop_id','stop_name','stop_lat','stop_lon'])['route_short_name'].apply('/'.join).reset_index(drop=False)
stops2.columns=['stopid','stopname','lat','lon','route']
stops2.to_csv(path+'stops.csv',index=False)






lehd=[]
for i in ['ct','nj','ny','pa']:
    lehd+=[pd.read_csv(path+'edc/edc4/'+i+'_wac_S000_JT00_2018.csv',dtype=float,converters={'w_geocode':str})]
lehd=pd.concat(lehd,axis=0,ignore_index=True)
lehd['blockid']=lehd['w_geocode'].copy()
lehd['jobs']=lehd['C000'].copy()
lehd=lehd[['blockid','jobs']].reset_index(drop=True)

df=[]
rd=pd.read_csv(path+'nyctract/resbk3.csv',dtype=float,converters={'blockid':str},chunksize=10000)
for ck in rd:   
    tp=pd.melt(ck,id_vars=['blockid'])
    tp=tp[tp['value']<=40].reset_index(drop=True)
    tp['orgct']=[x.replace('RES','') for x in tp['variable']]
    tp['destbk']=tp['blockid'].copy()
    tp['time']=tp['value'].copy()
    tp=tp[['orgct','destbk','time']].reset_index(drop=True)
    df+=[tp]
df=pd.concat(df,axis=0,ignore_index=True)

df=pd.merge(df,lehd,how='inner',left_on='destbk',right_on='blockid')
df=df.groupby(['orgct'],as_index=False).agg({'jobs':'sum'}).reset_index(drop=True)
df.to_csv(path+'edc/edc4/ctjobs.csv',index=False)

apikey=pd.read_csv('C:/Users/mayij/Desktop/DOC/GITHUB/td-acsapi/apikey.csv',header=None).loc[0,0]
nyc=['36005','36047','36061','36081','36085']
pop=[]
for i in nyc:
    rs=requests.get('https://api.census.gov/data/2018/acs/acs5/subject?get=NAME,group(S0101)&for=tract:*&in=state:'+i[:2]+' county:'+i[2:]+'&key='+apikey).json()
    rs=pd.DataFrame(rs)
    rs.columns=rs.loc[0]
    rs=rs.loc[1:].reset_index(drop=True)
    rs['geoid']=[x[9:] for x in rs['GEO_ID']]
    rs=rs[['geoid','S0101_C01_001E','S0101_C01_001M']].reset_index(drop=True)
    rs.columns=['CT','TT','TTM']
    pop+=[rs]
pop=pd.concat(pop,axis=0,ignore_index=True)
pop.to_csv(path+'edc/edc4/pop.csv',index=False)

df=pd.read_csv(path+'edc/edc4/ctjobs.csv',dtype=float,converters={'orgct':str})
pop=pd.read_csv(path+'edc/edc4/pop.csv',dtype=float,converters={'CT':str})
geoxwalk=pd.read_csv('C:/Users/mayij/Desktop/DOC/DCP2021/TRAVEL DEMAND MODEL/POP/GEOIDCROSSWALK.csv',dtype=str)
df=pd.merge(df,pop,how='inner',left_on='orgct',right_on='CT')
df=pd.merge(df,geoxwalk[['CensusTract2010','NTA','NTAName']].drop_duplicates(keep='first'),how='inner',left_on='orgct',right_on='CensusTract2010')
df['jobstt']=df['jobs']*df['TT']
df=df.groupby(['NTA'],as_index=False).agg({'jobstt':'sum','TT':'sum'}).reset_index(drop=True)
df['Jobs']=df['jobstt']/df['TT']
df=df[~np.isin(df['NTA'],['BK99','BX98','BX99','MN99','QN98','QN99','SI99'])].reset_index(drop=True)
df=df[['NTA','Jobs']].reset_index(drop=True)
df.columns=['NTACode','Jobs']
nta=gpd.read_file('C:/Users/mayij/Desktop/DOC/DCP2020/COVID19/SUBWAY/TURNSTILE/ntaclippedadj.shp')
nta.crs=4326
df=pd.merge(nta,df,how='inner',on='NTACode')
df.to_file(path+'edc/edc4/ntajobs.geojson',driver='GeoJSON')
df=df[['NTACode','NTAName','Jobs']].reset_index(drop=True)
df.to_csv(path+'edc/edc4/ntajobs.csv',index=False)







locations=pd.read_csv(path+'edc/edc4/locations.csv',dtype=str)
locations['facid']=range(1,len(locations)+1)
walkshed=gpd.read_file(path+'edc/edc4/walksheds/walksheds.shp')
walkshed.crs=4326
walkshed=pd.merge(walkshed,locations,how='inner',left_on='FacilityID',right_on='facid')
walkshed.to_file(path+'edc/edc4/walksheds/walksheds.geojson',driver='GeoJSON')

walkshed=gpd.read_file(path+'edc/edc4/walksheds/walksheds.geojson')
walkshed.crs=4326
walkshed['dist']=3960
walkshed=walkshed.dissolve(by='dist').reset_index(drop=False)
walkshed=walkshed[['dist','geometry']].reset_index(drop=True)
walkshed.to_file(path+'edc/edc4/walksheds/walkshedsdis.geojson',driver='GeoJSON')

walkshed=gpd.read_file(path+'edc/edc4/walksheds/walkshedsdis.geojson')
walkshed.crs=4326
walkshed=walkshed.to_crs(6539)
ct=gpd.read_file(path+'shp/quadstatectclipped.shp')
ct.crs=4326
ct=ct.to_crs(6539)
ct['county']=[str(x)[0:5] for x in ct['tractid']]
ct=ct[np.isin(ct['county'],['36005','36047','36061','36081','36085'])].reset_index(drop=True)
df=gpd.overlay(ct,walkshed,how='intersection')
df['area']=[x.area for x in df['geometry']]
df=df.groupby(['tractid'],as_index=False).agg({'area':'sum'}).reset_index(drop=True)
df=pd.merge(ct,df,how='left',on='tractid')
df['totalarea']=[x.area for x in df['geometry']]
df['pct']=df['area']/df['totalarea']
pop=pd.read_csv(path+'edc/edc4/pop.csv',dtype=float,converters={'CT':str})
geoxwalk=pd.read_csv('C:/Users/mayij/Desktop/DOC/DCP2021/TRAVEL DEMAND MODEL/POP/GEOIDCROSSWALK.csv',dtype=str)
df=pd.merge(df,pop,how='inner',left_on='tractid',right_on='CT')
df['pop']=df['pct']*df['TT']
df['totalpop']=df['TT'].copy()
df=pd.merge(df,geoxwalk,how='inner',left_on='tractid',right_on='CensusTract2010')
df=df.groupby(['NTA'],as_index=False).agg({'pop':'sum','totalpop':'sum'}).reset_index(drop=True)
df['pct']=df['pop']/df['totalpop']
df=df[~np.isin(df['NTA'],['BK99','BX98','BX99','MN99','QN98','QN99','SI99'])].reset_index(drop=True)
df=df[['NTA','pop','totalpop','pct']].reset_index(drop=True)
df.columns=['NTACode','TransitPop','TotalPop','Pct']
nta=gpd.read_file('C:/Users/mayij/Desktop/DOC/DCP2020/COVID19/SUBWAY/TURNSTILE/ntaclippedadj.shp')
nta.crs=4326
df=pd.merge(nta,df,how='inner',on='NTACode')
df.to_file(path+'edc/edc4/ntatransitpop.geojson',driver='GeoJSON')
df=df[['NTACode','NTAName','TransitPop','TotalPop','Pct']].reset_index(drop=True)
df.to_csv(path+'edc/edc4/ntatransitpop.csv',index=False)
