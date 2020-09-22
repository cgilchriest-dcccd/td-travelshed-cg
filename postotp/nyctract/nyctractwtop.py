#! /usr/bin/python3
# Travelshed Mapping
# OpenTripPlanner Guide: http://docs.opentripplanner.org/en/latest/
# OpenTripPlanner API Doc: http://dev.opentripplanner.org/apidoc/1.0.0/index.html

import datetime
import requests
import geopandas as gpd
import pandas as pd
import numpy as np
import multiprocessing as mp
import os

start=datetime.datetime.now()

pd.set_option('display.max_columns', None)
path='/home/mayijun/TRAVELSHED/'
#path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2018/TRAVELSHEDREVAMP/'
# path='C:/Users/mayij/Desktop/DOC/DCP2018/TRAVELSHEDREVAMP/'
 
# # Load quadstate blokc point shapefile
# bkpt=gpd.read_file(path+'shp/quadstatebkpt.shp')
# bkpt.crs={'init': 'epsg:4326'}

# # Set typical day
# typicaldate='2018/06/06'

# # Create arrival time list
# arrivaltimeinterval=15 # in minutes
# arrivaltimestart='12:00:00'
# arrivaltimeend='13:00:00'
# arrivaltimestart=datetime.datetime.strptime(arrivaltimestart,'%H:%M:%S')
# arrivaltimeend=datetime.datetime.strptime(arrivaltimeend,'%H:%M:%S')
# arrivaltimeincrement=arrivaltimestart
# arrivaltime=[]
# while arrivaltimeincrement<=arrivaltimeend:
#     arrivaltime.append(datetime.datetime.strftime(arrivaltimeincrement,'%H:%M:%S'))
#     arrivaltimeincrement+=datetime.timedelta(seconds=arrivaltimeinterval*60)

# # Set maximum number of transfers
# maxTransfers=3 # 4 boardings

# # Set maximum walking distance
# maxWalkDistance=805 # in meters

# # Set cut off points between 0-120 mins
# cutoffinterval=2 # in minutes
# cutoffstart=0
# cutoffend=120
# cutoffincrement=cutoffstart
# cutoff=''
# while cutoffincrement<cutoffend:
#     cutoff+='&cutoffSec='+str((cutoffincrement+cutoffinterval)*60)
#     cutoffincrement+=cutoffinterval

# # Definie res travelshed function to generate isochrones and spatial join to Census Blocks
# def restravelshed(arrt):
#     bk=bkpt.copy()
#     url='http://localhost:8801/otp/routers/default/isochrone?batch=true&mode=WALK,TRANSIT'
#     url+='&fromPlace='+destination.loc[i,'latlong']
#     url+='&date='+typicaldate+'&time='+arrt+'&maxTransfers='+str(maxTransfers)
#     url+='&maxWalkDistance='+str(maxWalkDistance)+'&clampInitialWait=0'+cutoff
#     headers={'Accept':'application/json'}  
#     req=requests.get(url=url,headers=headers)
#     js=req.json()
#     iso=gpd.GeoDataFrame.from_features(js,crs={'init': 'epsg:4326'})
#     bk['T'+arrt[0:2]+arrt[3:5]]=999
#     cut=range(cutoffend,cutoffstart,-cutoffinterval)
#     bkiso=gpd.sjoin(bk,iso.loc[iso['time']==cut[0]*60],how='left',op='within')
#     bkiso=bkiso.loc[pd.notnull(bkiso['time']),'blockid']
#     bk.loc[bk['blockid'].isin(bkiso),'T'+arrt[0:2]+arrt[3:5]]=cut[0]-cutoffinterval/2
#     for k in range(0,(len(cut)-1)):
#         if (iso.loc[iso['time']==cut[k+1]*60,'geometry'].notna()).bool():
#             if len(bk.loc[bk['T'+arrt[0:2]+arrt[3:5]]==cut[k]-cutoffinterval/2])!=0:
#                 try:
#                     bkiso=gpd.sjoin(bk.loc[bk['T'+arrt[0:2]+arrt[3:5]]==cut[k]-cutoffinterval/2],
#                                     iso.loc[iso['time']==cut[k+1]*60],how='left',op='within')
#                     bkiso=bkiso.loc[pd.notnull(bkiso['time']),'blockid']
#                     bk.loc[bk['blockid'].isin(bkiso),'T'+arrt[0:2]+arrt[3:5]]=cut[k+1]-cutoffinterval/2
#                 except ValueError:
#                     print(destination.loc[i,'id']+' '+arrt+' '+
#                           str(cut[k+1])+'-minute isochrone has no Census Block in it!')
#             else:
#                 print(destination.loc[i,'id']+' '+arrt+' '+
#                       str(cut[k])+'-minute isochrone has no Census Block in it!')
#         else:
#             print(destination.loc[i,'id']+' '+arrt+' '+
#                   str(cut[k+1])+'-minute isochrone has no geometry!')
#     bk['T'+arrt[0:2]+arrt[3:5]]=bk['T'+arrt[0:2]+arrt[3:5]].replace(999,np.nan)
#     bk=bk.drop(['lat','long','geometry'],axis=1)
#     bk=bk.set_index('blockid')
#     return bk


# # Define parallel multiprocessing function
# def parallelize(data, func):
#     pool = mp.Pool(len(data))
#     dt = pd.concat(pool.map(func, data),axis=1)
#     pool.close()
#     pool.join()
#     return dt

# # Multiprocessing travelshed function for NYC Res Census Tracts
# if __name__=='__main__':
#     location=pd.read_csv(path+'nyctract/centroid/nycrestractptop.csv',dtype=str)
#     location['id']=['RES'+str(x).zfill(11) for x in location['censustract']]
#     location['latlong']=[str(x)+','+str(y) for x,y in zip(location['resintlat'],location['resintlong'])]
#     destination=location.loc[0:max(location.count())-1,['id','latlong']]
#     for i in destination.index:
#         df=parallelize(arrivaltime,restravelshed)
#         df['TTMEDIAN']=df.median(skipna=True,axis=1)
#         df=df['TTMEDIAN'].sort_index()
#         df.name=destination.loc[i,'id']
#         df.to_csv(path+'nyctract/op/'+destination.loc[i,'id']+'.csv',index=True,header=True,na_rep=999)
#     print(datetime.datetime.now()-start)







# Summarize travelshed outputs
# NYC Res Censust Tracts
# resbk=pd.DataFrame()
# for i in sorted(os.listdir(path+'nyctract/op/'))[0:500]:
#     tp=pd.read_csv(path+'nyctract/op/'+i,dtype=str)
#     tp=tp.set_index('blockid')
#     resbk=pd.concat([resbk,tp],axis=1)
# resbk.to_csv(path+'nyctract/resbkop1.csv',index=True)
# resbk=pd.DataFrame()
# for i in sorted(os.listdir(path+'nyctract/op/'))[500:1000]:
#     tp=pd.read_csv(path+'nyctract/op/'+i,dtype=str)
#     tp=tp.set_index('blockid')
#     resbk=pd.concat([resbk,tp],axis=1)
# resbk.to_csv(path+'nyctract/resbkop2.csv',index=True)
# resbk=pd.DataFrame()
# for i in sorted(os.listdir(path+'nyctract/op/'))[1000:1500]:
#     tp=pd.read_csv(path+'nyctract/op/'+i,dtype=str)
#     tp=tp.set_index('blockid')
#     resbk=pd.concat([resbk,tp],axis=1)
# resbk.to_csv(path+'nyctract/resbkop3.csv',index=True)
# resbk=pd.DataFrame()
# for i in sorted(os.listdir(path+'nyctract/op/'))[1500:]:
#     tp=pd.read_csv(path+'nyctract/op/'+i,dtype=str)
#     tp=tp.set_index('blockid')
#     resbk=pd.concat([resbk,tp],axis=1)
# resbk.to_csv(path+'nyctract/resbkop4.csv',index=True)


# tp1=pd.read_csv(path+'nyctract/resbkop1.csv',dtype=str)
# tp1=tp1.set_index('blockid')
# tp1=tp1.transpose()
# tp1.to_csv(path+'nyctract/resbkop.csv',index=True,index_label='SITE',header=True,mode='w')
# tp2=pd.read_csv(path+'nyctract/resbkop2.csv',dtype=str)
# tp2=tp2.set_index('blockid')
# tp2=tp2.transpose()
# tp2.to_csv(path+'nyctract/resbkop.csv',index=True,header=False,mode='a')
# tp3=pd.read_csv(path+'nyctract/resbkop3.csv',dtype=str)
# tp3=tp3.set_index('blockid')
# tp3=tp3.transpose()
# tp3.to_csv(path+'nyctract/resbkop.csv',index=True,header=False,mode='a')
tp4=pd.read_csv(path+'nyctract/resbkop4.csv',dtype=str)
tp4=tp4.set_index('blockid')
tp4=tp4.transpose()
tp4.to_csv(path+'nyctract/resbkop.csv',index=True,header=False,mode='a')
# resbk=pd.read_csv(path+'nyctract/resbkop.csv',dtype=str)
# resbk=resbk.set_index('SITE')
# resbk=resbk.transpose()
# resbk.to_csv(path+'nyctract/resbkop5.csv',index=True,index_label='blockid',header=True,mode='w')


# resct=pd.read_csv(path+'nyctract/resbkop.csv',dtype=str)
# resct=resct.set_index('blockid')
# resloclist=sorted(resct.columns)
# for i in resct.columns:
#     resct[i]=pd.to_numeric(resct[i])
# resct=resct.replace(999,np.nan)
# resct['tractid']=[str(x)[0:11] for x in resct.index]
# resct=resct.groupby(['tractid'])[resloclist].median(skipna=True)
# resct.to_csv(path+'nyctract/resctop.csv',index=True,na_rep='999')