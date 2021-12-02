#! /usr/bin/python3
# Travelshed Mapping
# OpenTripPlanner Guide: http://docs.opentripplanner.org/en/latest/
# OpenTripPlanner API Doc: http://dev.opentripplanner.org/apidoc/1.0.0/index.html

import datetime
import time
import geopandas as gpd
import pandas as pd
import numpy as np
import shapely
import requests
import multiprocessing as mp
import plotly.graph_objects as go
import plotly.io as pio
import json



pd.set_option('display.max_columns', None)
pio.renderers.default='browser'
path='/home/mayijun/TRAVELSHED/'
# path='C:/Users/mayij/Desktop/DOC/DCP2018/TRAVELSHEDREVAMP/'
doserver='http://159.65.64.166:8801/'
doserver='http://localhost:8801/'



start=datetime.datetime.now()

# Set typical day
typicaldate='2018/06/06'

# Create arrival time list
arrivaltimeinterval=10 # in minutes
arrivaltimestart='07:00:00'
arrivaltimeend='10:00:00'
arrivaltimestart=datetime.datetime.strptime(arrivaltimestart,'%H:%M:%S')
arrivaltimeend=datetime.datetime.strptime(arrivaltimeend,'%H:%M:%S')
arrivaltimeincrement=arrivaltimestart
arrivaltime=[]
while arrivaltimeincrement<=arrivaltimeend:
    arrivaltime.append(datetime.datetime.strftime(arrivaltimeincrement,'%H:%M:%S'))
    arrivaltimeincrement+=datetime.timedelta(seconds=arrivaltimeinterval*60)

# Set maximum number of transfers
maxTransfers=3 # 4 boardings

# Set maximum walking distance
maxWalkDistance=805 # in meters

# Set cut off points between 0-120 mins
cutoffinterval=45 # in minutes
cutoffstart=0
cutoffend=45
cutoffincrement=cutoffstart
cutoff=''
while cutoffincrement<cutoffend:
    cutoff+='&cutoffSec='+str((cutoffincrement+cutoffinterval)*60)
    cutoffincrement+=cutoffinterval

# Definie res travelshed function to generate isochrones and spatial join to Census Blocks
def travelshedwt(arrt):
    tp=destination.loc[[i],['tractid']].reset_index(drop=True)   
    if destination.loc[i,'direction']=='to':
        url=doserver+'otp/routers/default/isochrone?batch=true&mode=WALK,TRANSIT'
        url+='&fromPlace='+destination.loc[i,'latlong']+'&toPlace='+destination.loc[i,'latlong']
        url+='&arriveBy=true&date='+typicaldate+'&time='+arrt+'&maxTransfers='+str(maxTransfers)
        url+='&maxWalkDistance='+str(maxWalkDistance)+'&clampInitialWait=-1'+cutoff
    elif destination.loc[i,'direction']=='from':
        url=doserver+'otp/routers/default/isochrone?batch=true&mode=WALK,TRANSIT'
        url+='&fromPlace='+destination.loc[i,'latlong']
        url+='&date='+typicaldate+'&time='+arrt+'&maxTransfers='+str(maxTransfers)
        url+='&maxWalkDistance='+str(maxWalkDistance)+'&clampInitialWait=0'+cutoff
    else:
        print(destination.loc[i,'tractid']+' has no direction!')
    headers={'Accept':'application/json'}
    req=requests.get(url=url,headers=headers)
    js=req.json()
    iso=gpd.GeoDataFrame.from_features(js,crs=4326)
    iso=iso.to_crs(6539)
    tp['T'+arrt[0:2]+arrt[3:5]]=0
    cut=range(cutoffend,cutoffstart,-cutoffinterval)
    if iso.loc[iso['time']==cut[0]*60,'geometry'].notna().bool():
        try:
            tp['T'+arrt[0:2]+arrt[3:5]]=iso.loc[0,'geometry'].area/43560
        except:
            print(destination.loc[i,'tractid']+' '+arrt+' '+str(cut[0])+'-minute isochrone has no Census Block in it!')
    else:
        print(destination.loc[i,'tractid']+' '+arrt+' '+str(cut[0])+'-minute isochrone has no geometry!')
    tp['T'+arrt[0:2]+arrt[3:5]]=tp['T'+arrt[0:2]+arrt[3:5]].replace(999,np.nan)
    tp=tp.set_index('tractid')
    return tp



## Define parallel multiprocessing function
def parallelize(data, func):
    data_split=np.array_split(data,np.ceil(len(data)/(mp.cpu_count()-4)))
    pool=mp.Pool(mp.cpu_count()-4)
    dt=pd.DataFrame()
    for i in data_split:
        ds=pd.concat(pool.map(func,i),axis=1)
        dt=pd.concat([dt,ds],axis=1)
    pool.close()
    pool.join()
    return dt



# Multiprocessing travelshed function for sites
if __name__=='__main__':
    location=pd.read_excel(path+'nyctract/centroid/centroid.xlsx',sheet_name='nycrestractptadjfinal',dtype=str)
    location['tractid']=location['censustract'].copy()
    location['latlong']=[str(x)+','+str(y) for x,y in zip(location['resintlatfinal'],location['resintlongfinal'])]
    location['direction']='from'
    location['acre60']=0
    destination=location.loc[2160:max(location.count())-1,['tractid','direction','latlong','acre60']].reset_index(drop=True)
    for i in destination.index:
        df=parallelize(arrivaltime,travelshedwt)
        df['TTMEDIAN']=df.median(skipna=True,axis=1)
        df=list(df['TTMEDIAN'])[0]
        destination.loc[i,'acre60']=df
    destination['std']=(destination['acre60']-np.mean(destination['acre60']))/np.std(destination['acre60'])
    destination['stdcat']=np.where(destination['std']>=2.5,'>=+2.5SD',
                          np.where(destination['std']>=1.5,'+1.5SD ~ +2.5SD',
                          np.where(destination['std']>=0.5,'+0.5SD ~ +1.5SD',
                          np.where(destination['std']>=-0.5,'-0.5SD ~ +0.5SD',
                          np.where(destination['std']>=-1.5,'-1.5SD ~ -0.5SD',
                          np.where(destination['std']>=-2.5,'-2.5SD ~ -1.5SD','<-2.5SD'))))))
    destination['pct']=pd.qcut(destination['acre60'],100,labels=False)
    destination=destination[['tractid','acre60','std','stdcat','pct']].reset_index(drop=True)
    destination.to_csv(path+'mobility/isofrom.csv',index=False)
    ct=gpd.read_file(path+'shp/quadstatectclipped.shp')
    ct.crs=4326
    ct=ct[['tractid','geometry']].reset_index(drop=True)
    destination=pd.merge(ct,destination,how='inner',on='tractid')
    destination.to_file(path+'mobility/isofrom.shp')
    print(datetime.datetime.now()-start)

