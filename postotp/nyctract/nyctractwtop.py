#! /usr/bin/python3
# Travelshed Mapping
# OpenTripPlanner Guide: http://docs.opentripplanner.org/en/latest/
# OpenTripPlanner API Doc: http://dev.opentripplanner.org/apidoc/1.0.0/index.html

import matplotlib
matplotlib.use('Agg')

import datetime
import time
import geopandas as gpd
import pandas as pd
import numpy as np
import shapely
import requests
import multiprocessing as mp



# Definie res travelshed function to generate isochrones and spatial join to Census Blocks
def travelshedwt(arrt):
    bk=bkpt.copy()
    if destination.loc[i,'direction']=='in':
        url=doserver+'otp/routers/default/isochrone?batch=true&mode=WALK,TRANSIT'
        url+='&fromPlace='+destination.loc[i,'latlong']+'&toPlace='+destination.loc[i,'latlong']
        url+='&arriveBy=true&date='+typicaldate+'&time='+arrt+'&maxTransfers='+str(maxTransfers)
        url+='&maxWalkDistance='+str(maxWalkDistance)+'&clampInitialWait=-1'+cutoff
        headers={'Accept':'application/json'}  
        req=requests.get(url=url,headers=headers)
        js=req.json()
        iso=gpd.GeoDataFrame.from_features(js,crs={'init': 'epsg:4326'})
        bk['T'+arrt[0:2]+arrt[3:5]]=999
        cut=range(cutoffend,cutoffstart,-cutoffinterval)
        if (iso.loc[iso['time']==cut[0]*60,'geometry'].notna()).bool():
            try:
                bkiso=gpd.sjoin(bk,iso.loc[iso['time']==cut[0]*60],how='left',op='within')
                bkiso=bkiso.loc[pd.notnull(bkiso['time']),'blockid']
                bk.loc[bk['blockid'].isin(bkiso),'T'+arrt[0:2]+arrt[3:5]]=cut[0]-cutoffinterval/2
            except ValueError:
                print(destination.loc[i,'id']+' '+arrt+' '+
                      str(cut[0])+'-minute isochrone has no Census Block in it!')
            for k in range(0,(len(cut)-1)):
                if (iso.loc[iso['time']==cut[k+1]*60,'geometry'].notna()).bool():
                    if len(bk.loc[bk['T'+arrt[0:2]+arrt[3:5]]==cut[k]-cutoffinterval/2])!=0:
                        try:
                            bkiso=gpd.sjoin(bk.loc[bk['T'+arrt[0:2]+arrt[3:5]]==cut[k]-cutoffinterval/2],
                                            iso.loc[iso['time']==cut[k+1]*60],how='left',op='within')
                            bkiso=bkiso.loc[pd.notnull(bkiso['time']),'blockid']
                            bk.loc[bk['blockid'].isin(bkiso),'T'+arrt[0:2]+arrt[3:5]]=cut[k+1]-cutoffinterval/2
                        except ValueError:
                            print(destination.loc[i,'id']+' '+arrt+' '+
                                  str(cut[k+1])+'-minute isochrone has no Census Block in it!')
                    else:
                        print(destination.loc[i,'id']+' '+arrt+' '+
                              str(cut[k])+'-minute isochrone has no Census Block in it!')
                else:
                    print(destination.loc[i,'id']+' '+arrt+' '+
                          str(cut[k+1])+'-minute isochrone has no geometry!')
        else:
            print(destination.loc[i,'id']+' '+arrt+' '+
                  str(cut[0])+'-minute isochrone has no geometry!')
        bk['T'+arrt[0:2]+arrt[3:5]]=bk['T'+arrt[0:2]+arrt[3:5]].replace(999,np.nan)
        bk=bk.drop(['lat','long','geometry'],axis=1)
        bk=bk.set_index('blockid')
        return bk
    elif destination.loc[i,'direction']=='out':
        url=doserver+'otp/routers/default/isochrone?batch=true&mode=WALK,TRANSIT'
        url+='&fromPlace='+destination.loc[i,'latlong']
        url+='&date='+typicaldate+'&time='+arrt+'&maxTransfers='+str(maxTransfers)
        url+='&maxWalkDistance='+str(maxWalkDistance)+'&clampInitialWait=0'+cutoff
        headers={'Accept':'application/json'}  
        req=requests.get(url=url,headers=headers)
        js=req.json()
        iso=gpd.GeoDataFrame.from_features(js,crs={'init': 'epsg:4326'})
        bk['T'+arrt[0:2]+arrt[3:5]]=999
        cut=range(cutoffend,cutoffstart,-cutoffinterval)
        if (iso.loc[iso['time']==cut[0]*60,'geometry'].notna()).bool():
            try:     
                bkiso=gpd.sjoin(bk,iso.loc[iso['time']==cut[0]*60],how='left',op='within')
                bkiso=bkiso.loc[pd.notnull(bkiso['time']),'blockid']
                bk.loc[bk['blockid'].isin(bkiso),'T'+arrt[0:2]+arrt[3:5]]=cut[0]-cutoffinterval/2
            except ValueError:
                print(destination.loc[i,'id']+' '+arrt+' '+
                      str(cut[0])+'-minute isochrone has no Census Block in it!')            
            for k in range(0,(len(cut)-1)):
                if (iso.loc[iso['time']==cut[k+1]*60,'geometry'].notna()).bool():
                    if len(bk.loc[bk['T'+arrt[0:2]+arrt[3:5]]==cut[k]-cutoffinterval/2])!=0:
                        try:
                            bkiso=gpd.sjoin(bk.loc[bk['T'+arrt[0:2]+arrt[3:5]]==cut[k]-cutoffinterval/2],
                                            iso.loc[iso['time']==cut[k+1]*60],how='left',op='within')
                            bkiso=bkiso.loc[pd.notnull(bkiso['time']),'blockid']
                            bk.loc[bk['blockid'].isin(bkiso),'T'+arrt[0:2]+arrt[3:5]]=cut[k+1]-cutoffinterval/2
                        except ValueError:
                            print(destination.loc[i,'id']+' '+arrt+' '+
                                  str(cut[k+1])+'-minute isochrone has no Census Block in it!')
                    else:
                        print(destination.loc[i,'id']+' '+arrt+' '+
                              str(cut[k])+'-minute isochrone has no Census Block in it!')
                else:
                    print(destination.loc[i,'id']+' '+arrt+' '+
                          str(cut[k+1])+'-minute isochrone has no geometry!')
        else:
            print(destination.loc[i,'id']+' '+arrt+' '+
                  str(cut[0])+'-minute isochrone has no geometry!')        
        bk['T'+arrt[0:2]+arrt[3:5]]=bk['T'+arrt[0:2]+arrt[3:5]].replace(999,np.nan)
        bk=bk.drop(['lat','long','geometry'],axis=1)
        bk=bk.set_index('blockid')
        return bk


# Define parallel multiprocessing function
def parallelize(data, func):
    data_split=np.array_split(data,np.ceil(len(data)/(mp.cpu_count()-1)))
    pool=mp.Pool(mp.cpu_count()-1)
    dt=pd.DataFrame()
    for i in data_split:
        ds=pd.concat(pool.map(func,i),axis=1)
        dt=pd.concat([dt,ds],axis=1)
    pool.close()
    pool.join()
    return dt



# Multiprocessing travelshed function for sites
if __name__=='__main__':
    start=datetime.datetime.now()
    pd.set_option('display.max_columns', None)
    path='/home/mayijun/TRAVELSHED/'
    # path='C:/Users/mayij/Desktop/DOC/DCP2018/TRAVELSHEDREVAMP/'
    #path='C:/Users/Y_Ma2/Desktop/amazon/'
    #doserver='http://142.93.21.138:8801/'
    doserver='http://localhost:8801/'
    # Load quadstate block point shapefile
    bkpt=gpd.read_file(path+'shp/quadstatebkpt.shp')
    bkpt.crs={'init': 'epsg:4326'}
    # Set typical day
    typicaldate='2018/06/06'
    # Create arrival time list
    arrivaltimeinterval=15 # in minutes
    arrivaltimestart='12:00:00'
    arrivaltimeend='13:00:00'
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
    cutoffinterval=2 # in minutes
    cutoffstart=0
    cutoffend=120
    cutoffincrement=cutoffstart
    cutoff=''
    while cutoffincrement<cutoffend:
        cutoff+='&cutoffSec='+str((cutoffincrement+cutoffinterval)*60)
        cutoffincrement+=cutoffinterval
    location=pd.read_excel(path+'perrequest/input.xlsx',sheet_name='input',dtype=str)
    location['id']=['SITE'+str(x).zfill(3) for x in location['siteid']]
    location['latlong']=[str(x)+','+str(y) for x,y in zip(location['intlat'],location['intlong'])]
    destination=location.loc[0:max(location.count())-1,['id','direction','latlong']]
    # Create travel time table for each site
    for i in destination.index:
        df=parallelize(arrivaltime,travelshedwt)
        df['TTMEDIAN']=df.median(skipna=True,axis=1)
        df=df['TTMEDIAN'].sort_index()
        df.name=destination.loc[i,'id']
        df.to_csv(path+'perrequest/'+destination.loc[i,'id']+'wt.csv',index=True,header=True,na_rep=999)
 