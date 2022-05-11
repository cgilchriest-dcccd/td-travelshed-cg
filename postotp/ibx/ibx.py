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
import os



pd.set_option('display.max_columns', None)
pio.renderers.default='browser'
path='/home/mayijun/TRAVELSHED/'
# path='C:/Users/mayij/Desktop/DOC/DCP2018/TRAVELSHEDREVAMP/'
# doserver='http://159.65.64.166:8801/'
doserver='http://localhost:8801/'



start=datetime.datetime.now()

# Load quadstate block point shapefile
bkpt=gpd.read_file(path+'shp/quadstatebkpt.shp')
bkpt.crs=4326
bkpt['state']=[str(x)[0:2] for x in bkpt['blockid']]
bkpt=bkpt[np.isin(bkpt['state'],['36','34'])].reset_index(drop=True)
bkpt=bkpt.drop(['state','lat','long'],axis=1).reset_index(drop=True)

# Set typical day
typicaldate='2018/06/06'

# Create arrival time list
arrivaltimeinterval=60 # in minutes
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
cutoffinterval=2 # in minutes
cutoffstart=0
cutoffend=60
cutoffincrement=cutoffstart
cutoff=''
while cutoffincrement<cutoffend:
    cutoff+='&cutoffSec='+str((cutoffincrement+cutoffinterval)*60)
    cutoffincrement+=cutoffinterval

# Definie res travelshed function to generate isochrones and spatial join to Census Blocks
def travelshedwt(arrt):
    bk=bkpt.copy()
    if destination.loc[i,'direction']=='to':
        url=doserver+'otp/routers/default/isochrone?batch=true&mode=WALK,TRANSIT'
        url+='&fromPlace='+destination.loc[i,'latlong']+'&toPlace='+destination.loc[i,'latlong']
        url+='&arriveBy=true&date='+typicaldate+'&time='+arrt+'&maxTransfers='+str(maxTransfers)
        url+='&maxWalkDistance='+str(maxWalkDistance)+'&clampInitialWait=-1'+cutoff
        # url+='&bannedAgencies=IBX'
    elif destination.loc[i,'direction']=='from':
        url=doserver+'otp/routers/default/isochrone?batch=true&mode=WALK,TRANSIT'
        url+='&fromPlace='+destination.loc[i,'latlong']
        url+='&date='+typicaldate+'&time='+arrt+'&maxTransfers='+str(maxTransfers)
        url+='&maxWalkDistance='+str(maxWalkDistance)+'&clampInitialWait=0'+cutoff
        # url+='&bannedAgencies=IBX'
    else:
        print(destination.loc[i,'tractid']+' has no direction!')
    headers={'Accept':'application/json'}
    req=requests.get(url=url,headers=headers)
    js=req.json()
    iso=gpd.GeoDataFrame.from_features(js,crs=4326)
    bk['T'+arrt[0:2]+arrt[3:5]]=999
    cut=range(cutoffend,cutoffstart,-cutoffinterval)
    if iso.loc[iso['time']==cut[0]*60,'geometry'].notna().bool():
        try:
            bkiso=gpd.sjoin(bk,iso.loc[iso['time']==cut[0]*60],how='inner',op='within')
            bkiso=bkiso['blockid']
            bk.loc[bk['blockid'].isin(bkiso),'T'+arrt[0:2]+arrt[3:5]]=cut[0]-cutoffinterval/2
        except:
            print(destination.loc[i,'tractid']+' '+arrt+' '+str(cut[0])+'-minute isochrone has no Census Block in it!')
        for k in range(0,(len(cut)-1)):
            if iso.loc[iso['time']==cut[k+1]*60,'geometry'].notna().bool():
                if len(bk.loc[bk['T'+arrt[0:2]+arrt[3:5]]==cut[k]-cutoffinterval/2])!=0:
                    try:
                        bkiso=gpd.sjoin(bk.loc[bk['T'+arrt[0:2]+arrt[3:5]]==cut[k]-cutoffinterval/2],
                                        iso.loc[iso['time']==cut[k+1]*60],how='inner',op='within')
                        bkiso=bkiso['blockid']
                        bk.loc[bk['blockid'].isin(bkiso),'T'+arrt[0:2]+arrt[3:5]]=cut[k+1]-cutoffinterval/2
                    except ValueError:
                        print(destination.loc[i,'tractid']+' '+arrt+' '+str(cut[k+1])+'-minute isochrone has no Census Block in it!')
                else:
                    print(destination.loc[i,'tractid']+' '+arrt+' '+str(cut[k])+'-minute isochrone has no Census Block in it!')
            else:
                print(destination.loc[i,'tractid']+' '+arrt+' '+str(cut[k+1])+'-minute isochrone has no geometry!')
    else:
        print(destination.loc[i,'tractid']+' '+arrt+' '+str(cut[0])+'-minute isochrone has no geometry!')
    bk['T'+arrt[0:2]+arrt[3:5]]=bk['T'+arrt[0:2]+arrt[3:5]].replace(999,np.nan)
    bk=bk.drop(['geometry'],axis=1)
    bk=bk.set_index('blockid')
    return bk



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
    # location=pd.read_excel(path+'nyctract/centroid/centroid.xlsx',sheet_name='nycrestractptadjfinal',dtype=str)
    # location['tractid']=location['censustract'].copy()
    # location['latlong']=[str(x)+','+str(y) for x,y in zip(location['resintlatfinal'],location['resintlongfinal'])]
    # location['direction']='from'
    # destination=location.loc[0:max(location.count())-1,['tractid','direction','latlong']].reset_index(drop=True)
    location=pd.read_excel(path+'nyctract/centroid/centroid.xlsx',sheet_name='nycworktractptadjfinal',dtype=str)
    location['tractid']=location['censustract'].copy()
    location['latlong']=[str(x)+','+str(y) for x,y in zip(location['workintlatfinal'],location['workintlongfinal'])]
    location['direction']='to'
    destination=location.loc[0:max(location.count())-1,['tractid','direction','latlong']].reset_index(drop=True)
    # Create travel time table for each site
    # for i in destination.index:
    #     df=parallelize(arrivaltime,travelshedwt)
    #     df['TTMEDIAN']=df.median(skipna=True,axis=1)
    #     df=df['TTMEDIAN'].sort_index()
    #     df.name=destination.loc[i,'tractid']
    #     df.to_csv(path+'ibx/topost/'+destination.loc[i,'tractid']+'wt.csv',index=True,header=True,na_rep=999)
        
    # # Summarize travelshed outputs
    # # NYC Res Censust Tracts
    # resbk=pd.DataFrame()
    # for i in sorted(os.listdir(path+'ibx/topre/'))[0:500]:
    #     tp=pd.read_csv(path+'ibx/topre/'+i,dtype=str)
    #     tp=tp.set_index('blockid')
    #     resbk=pd.concat([resbk,tp],axis=1)
    # resbk.to_csv(path+'ibx/topre1.csv',index=True)
    # resbk=pd.DataFrame()
    # for i in sorted(os.listdir(path+'ibx/topre/'))[500:1000]:
    #     tp=pd.read_csv(path+'ibx/topre/'+i,dtype=str)
    #     tp=tp.set_index('blockid')
    #     resbk=pd.concat([resbk,tp],axis=1)
    # resbk.to_csv(path+'ibx/topre2.csv',index=True)
    # resbk=pd.DataFrame()
    # for i in sorted(os.listdir(path+'ibx/topre/'))[1000:1500]:
    #     tp=pd.read_csv(path+'ibx/topre/'+i,dtype=str)
    #     tp=tp.set_index('blockid')
    #     resbk=pd.concat([resbk,tp],axis=1)
    # resbk.to_csv(path+'ibx/topre3.csv',index=True)
    # resbk=pd.DataFrame()
    # for i in sorted(os.listdir(path+'ibx/topre/'))[1500:2000]:
    #     tp=pd.read_csv(path+'ibx/topre/'+i,dtype=str)
    #     tp=tp.set_index('blockid')
    #     resbk=pd.concat([resbk,tp],axis=1)
    # resbk.to_csv(path+'ibx/topre4.csv',index=True)
    # resbk=pd.DataFrame()
    # for i in sorted(os.listdir(path+'ibx/topre/'))[2000:]:
    #     tp=pd.read_csv(path+'ibx/topre/'+i,dtype=str)
    #     tp=tp.set_index('blockid')
    #     resbk=pd.concat([resbk,tp],axis=1)
    # resbk.to_csv(path+'ibx/topre5.csv',index=True)
    

    # for i in range(1,6):
    #     resct=pd.read_csv(path+'ibx/topre'+str(i)+'.csv',dtype=float,converters={'blockid':str})
    #     resct=resct.set_index('blockid')
    #     resloclist=sorted(resct.columns)
    #     resct=resct.replace(999,np.nan)
    #     resct['tractid']=[str(x)[0:11] for x in resct.index]
    #     resct=resct.groupby(['tractid'])[resloclist].median()
    #     resct.to_csv(path+'ibx/toprect'+str(i)+'.csv',index=True,na_rep='999')
        
    # resct=pd.DataFrame()
    # for i in range(1,6):
    #     tp=pd.read_csv(path+'ibx/toprect'+str(i)+'.csv',dtype=str)
    #     tp=tp.set_index('tractid')
    #     resct=pd.concat([resct,tp],axis=1)
    # resct.to_csv(path+'ibx/toprect.csv',index=True)
        
    
        
    # # Block Level Gravity Model
    # # Res Gravity
    # wac=pd.DataFrame()
    # for i in ['nj','ny']:
    #     tp=pd.read_csv(path+'lehd/'+str(i)+'_wac_S000_JT00_2019.csv',dtype=float,converters={'w_geocode':str})
    #     tp=tp[['w_geocode','C000']]
    #     wac=pd.concat([wac,tp],axis=0)
    # wac.columns=['blockid','wac']
    # wac=wac.set_index('blockid')
    
    # df=pd.DataFrame()
    # for k in range(1,6):
    #     resbkwac=pd.read_csv(path+'ibx/frompre'+str(k)+'.csv',dtype=float,converters={'blockid':str})
    #     resbkwac=resbkwac.set_index('blockid')
    #     resloclist=sorted(resbkwac.columns)
    #     resbkwac=pd.merge(resbkwac,wac,how='left',left_index=True,right_index=True)
    #     resbkwac['wac']=resbkwac['wac'].replace(np.nan,0)
    #     for i in resloclist:
    #         resbkwac[i]=np.where(resbkwac[i]<=5,2.5,
    #                     np.where(resbkwac[i]<=10,7.5,
    #                     np.where(resbkwac[i]<=15,12.5,
    #                     np.where(resbkwac[i]<=20,17.5,
    #                     np.where(resbkwac[i]<=25,22.5,
    #                     np.where(resbkwac[i]<=30,27.5,
    #                     np.where(resbkwac[i]<=35,32.5,
    #                     np.where(resbkwac[i]<=40,37.5,
    #                     np.where(resbkwac[i]<=45,42.5,
    #                     np.where(resbkwac[i]<=50,47.5,
    #                     np.where(resbkwac[i]<=55,52.5,
    #                     np.where(resbkwac[i]<=60,57.5,
    #                     np.where(resbkwac[i]<=65,62.5,
    #                     np.where(resbkwac[i]<=70,67.5,
    #                     np.where(resbkwac[i]<=75,72.5,
    #                     np.where(resbkwac[i]<=80,77.5,
    #                     np.where(resbkwac[i]<=85,82.5,
    #                     np.where(resbkwac[i]<=90,87.5,
    #                     np.where(resbkwac[i]<=95,92.5,
    #                     np.where(resbkwac[i]<=100,97.5,
    #                     np.where(resbkwac[i]<=105,102.5,
    #                     np.where(resbkwac[i]<=110,107.5,
    #                     np.where(resbkwac[i]<=115,112.5,
    #                     np.where(resbkwac[i]<=120,117.5,
    #                     np.nan))))))))))))))))))))))))
    #     resbkgravity=pd.DataFrame(index=resloclist,columns=['WAC1-5','WAC6-10','WAC11-15','WAC16-20','WAC21-25','WAC26-30',
    #                                                         'WAC31-35','WAC36-40','WAC41-45','WAC46-50','WAC51-55','WAC56-60',
    #                                                         'WAC61-65','WAC66-70','WAC71-75','WAC76-80','WAC81-85','WAC86-90',
    #                                                         'WAC91-95','WAC96-100','WAC101-105','WAC106-110','WAC111-115','WAC116-120',
    #                                                         'GWAC1-10','GWAC11-20','GWAC21-30','GWAC31-40','GWAC41-50','GWAC51-60',
    #                                                         'GRAVITYWAC'])
    #     for i in resloclist:
    #         resbkgravity.loc[i,'WAC1-5']=sum(resbkwac.loc[resbkwac[i]==2.5,'wac'])
    #         resbkgravity.loc[i,'WAC6-10']=sum(resbkwac.loc[resbkwac[i]==7.5,'wac'])
    #         resbkgravity.loc[i,'WAC11-15']=sum(resbkwac.loc[resbkwac[i]==12.5,'wac'])
    #         resbkgravity.loc[i,'WAC16-20']=sum(resbkwac.loc[resbkwac[i]==17.5,'wac'])
    #         resbkgravity.loc[i,'WAC21-25']=sum(resbkwac.loc[resbkwac[i]==22.5,'wac'])
    #         resbkgravity.loc[i,'WAC26-30']=sum(resbkwac.loc[resbkwac[i]==27.5,'wac'])
    #         resbkgravity.loc[i,'WAC31-35']=sum(resbkwac.loc[resbkwac[i]==32.5,'wac'])
    #         resbkgravity.loc[i,'WAC36-40']=sum(resbkwac.loc[resbkwac[i]==37.5,'wac'])
    #         resbkgravity.loc[i,'WAC41-45']=sum(resbkwac.loc[resbkwac[i]==42.5,'wac'])
    #         resbkgravity.loc[i,'WAC46-50']=sum(resbkwac.loc[resbkwac[i]==47.5,'wac'])
    #         resbkgravity.loc[i,'WAC51-55']=sum(resbkwac.loc[resbkwac[i]==52.5,'wac'])
    #         resbkgravity.loc[i,'WAC56-60']=sum(resbkwac.loc[resbkwac[i]==57.5,'wac'])
    #         resbkgravity.loc[i,'WAC61-65']=sum(resbkwac.loc[resbkwac[i]==62.5,'wac'])
    #         resbkgravity.loc[i,'WAC66-70']=sum(resbkwac.loc[resbkwac[i]==67.5,'wac'])
    #         resbkgravity.loc[i,'WAC71-75']=sum(resbkwac.loc[resbkwac[i]==72.5,'wac'])
    #         resbkgravity.loc[i,'WAC76-80']=sum(resbkwac.loc[resbkwac[i]==77.5,'wac'])
    #         resbkgravity.loc[i,'WAC81-85']=sum(resbkwac.loc[resbkwac[i]==82.5,'wac'])
    #         resbkgravity.loc[i,'WAC86-90']=sum(resbkwac.loc[resbkwac[i]==87.5,'wac'])
    #         resbkgravity.loc[i,'WAC91-95']=sum(resbkwac.loc[resbkwac[i]==92.5,'wac'])
    #         resbkgravity.loc[i,'WAC96-100']=sum(resbkwac.loc[resbkwac[i]==97.5,'wac'])
    #         resbkgravity.loc[i,'WAC101-105']=sum(resbkwac.loc[resbkwac[i]==102.5,'wac'])
    #         resbkgravity.loc[i,'WAC106-110']=sum(resbkwac.loc[resbkwac[i]==107.5,'wac'])
    #         resbkgravity.loc[i,'WAC111-115']=sum(resbkwac.loc[resbkwac[i]==112.5,'wac'])
    #         resbkgravity.loc[i,'WAC116-120']=sum(resbkwac.loc[resbkwac[i]==117.5,'wac'])
    #         resbkgravity.loc[i,'GWAC1-10']=(resbkgravity.loc[i,'WAC1-5']+resbkgravity.loc[i,'WAC6-10'])/(5**2)
    #         resbkgravity.loc[i,'GWAC11-20']=(resbkgravity.loc[i,'WAC11-15']+resbkgravity.loc[i,'WAC16-20'])/(15**2)
    #         resbkgravity.loc[i,'GWAC21-30']=(resbkgravity.loc[i,'WAC21-25']+resbkgravity.loc[i,'WAC26-30'])/(25**2)
    #         resbkgravity.loc[i,'GWAC31-40']=(resbkgravity.loc[i,'WAC31-35']+resbkgravity.loc[i,'WAC36-40'])/(35**2)
    #         resbkgravity.loc[i,'GWAC41-50']=(resbkgravity.loc[i,'WAC41-45']+resbkgravity.loc[i,'WAC46-50'])/(45**2)
    #         resbkgravity.loc[i,'GWAC51-60']=(resbkgravity.loc[i,'WAC51-55']+resbkgravity.loc[i,'WAC56-60'])/(55**2)
    #         resbkgravity.loc[i,'GRAVITYWAC']=resbkgravity.loc[i,'GWAC1-10']+resbkgravity.loc[i,'GWAC11-20']+resbkgravity.loc[i,'GWAC21-30']+resbkgravity.loc[i,'GWAC31-40']+resbkgravity.loc[i,'GWAC41-50']+resbkgravity.loc[i,'GWAC51-60']
    #     df=pd.concat([df,resbkgravity],axis=0)
    # df.to_csv(path+'ibx/frompregravity.csv',index=True)
        
    
    # # Work Gravity
    # rac=pd.DataFrame()
    # for i in ['nj','ny']:
    #     tp=pd.read_csv(path+'lehd/'+str(i)+'_rac_S000_JT01_2019.csv',dtype=float,converters={'h_geocode':str})
    #     tp=tp[['h_geocode','C000']]
    #     rac=pd.concat([rac,tp],axis=0)
    # rac.columns=['blockid','rac']
    # rac=rac.set_index('blockid')
    
    # df=pd.DataFrame()
    # for k in range(1,6):
    #     workbkrac=pd.read_csv(path+'ibx/topost'+str(k)+'.csv',dtype=float,converters={'blockid':str})
    #     workbkrac=workbkrac.set_index('blockid')
    #     workloclist=sorted(workbkrac.columns)
    #     workbkrac=pd.merge(workbkrac,rac,how='left',left_index=True,right_index=True)
    #     workbkrac['rac']=workbkrac['rac'].replace(np.nan,0)
    #     for i in workloclist:
    #         workbkrac[i]=np.where(workbkrac[i]<=5,2.5,
    #                     np.where(workbkrac[i]<=10,7.5,
    #                     np.where(workbkrac[i]<=15,12.5,
    #                     np.where(workbkrac[i]<=20,17.5,
    #                     np.where(workbkrac[i]<=25,22.5,
    #                     np.where(workbkrac[i]<=30,27.5,
    #                     np.where(workbkrac[i]<=35,32.5,
    #                     np.where(workbkrac[i]<=40,37.5,
    #                     np.where(workbkrac[i]<=45,42.5,
    #                     np.where(workbkrac[i]<=50,47.5,
    #                     np.where(workbkrac[i]<=55,52.5,
    #                     np.where(workbkrac[i]<=60,57.5,
    #                     np.where(workbkrac[i]<=65,62.5,
    #                     np.where(workbkrac[i]<=70,67.5,
    #                     np.where(workbkrac[i]<=75,72.5,
    #                     np.where(workbkrac[i]<=80,77.5,
    #                     np.where(workbkrac[i]<=85,82.5,
    #                     np.where(workbkrac[i]<=90,87.5,
    #                     np.where(workbkrac[i]<=95,92.5,
    #                     np.where(workbkrac[i]<=100,97.5,
    #                     np.where(workbkrac[i]<=105,102.5,
    #                     np.where(workbkrac[i]<=110,107.5,
    #                     np.where(workbkrac[i]<=115,112.5,
    #                     np.where(workbkrac[i]<=120,117.5,
    #                     np.nan))))))))))))))))))))))))
    #     workbkgravity=pd.DataFrame(index=workloclist,columns=['RAC1-5','RAC6-10','RAC11-15','RAC16-20','RAC21-25','RAC26-30',
    #                                                           'RAC31-35','RAC36-40','RAC41-45','RAC46-50','RAC51-55','RAC56-60',
    #                                                           'RAC61-65','RAC66-70','RAC71-75','RAC76-80','RAC81-85','RAC86-90',
    #                                                           'RAC91-95','RAC96-100','RAC101-105','RAC106-110','RAC111-115','RAC116-120',
    #                                                           'GRAC1-10','GRAC11-20','GRAC21-30','GRAC31-40','GRAC41-50','GRAC51-60',
    #                                                           'GRAVITYRAC'])
    #     for i in workloclist:
    #         workbkgravity.loc[i,'RAC1-5']=sum(workbkrac.loc[workbkrac[i]==2.5,'rac'])
    #         workbkgravity.loc[i,'RAC6-10']=sum(workbkrac.loc[workbkrac[i]==7.5,'rac'])
    #         workbkgravity.loc[i,'RAC11-15']=sum(workbkrac.loc[workbkrac[i]==12.5,'rac'])
    #         workbkgravity.loc[i,'RAC16-20']=sum(workbkrac.loc[workbkrac[i]==17.5,'rac'])
    #         workbkgravity.loc[i,'RAC21-25']=sum(workbkrac.loc[workbkrac[i]==22.5,'rac'])
    #         workbkgravity.loc[i,'RAC26-30']=sum(workbkrac.loc[workbkrac[i]==27.5,'rac'])
    #         workbkgravity.loc[i,'RAC31-35']=sum(workbkrac.loc[workbkrac[i]==32.5,'rac'])
    #         workbkgravity.loc[i,'RAC36-40']=sum(workbkrac.loc[workbkrac[i]==37.5,'rac'])
    #         workbkgravity.loc[i,'RAC41-45']=sum(workbkrac.loc[workbkrac[i]==42.5,'rac'])
    #         workbkgravity.loc[i,'RAC46-50']=sum(workbkrac.loc[workbkrac[i]==47.5,'rac'])
    #         workbkgravity.loc[i,'RAC51-55']=sum(workbkrac.loc[workbkrac[i]==52.5,'rac'])
    #         workbkgravity.loc[i,'RAC56-60']=sum(workbkrac.loc[workbkrac[i]==57.5,'rac'])
    #         workbkgravity.loc[i,'RAC61-65']=sum(workbkrac.loc[workbkrac[i]==62.5,'rac'])
    #         workbkgravity.loc[i,'RAC66-70']=sum(workbkrac.loc[workbkrac[i]==67.5,'rac'])
    #         workbkgravity.loc[i,'RAC71-75']=sum(workbkrac.loc[workbkrac[i]==72.5,'rac'])
    #         workbkgravity.loc[i,'RAC76-80']=sum(workbkrac.loc[workbkrac[i]==77.5,'rac'])
    #         workbkgravity.loc[i,'RAC81-85']=sum(workbkrac.loc[workbkrac[i]==82.5,'rac'])
    #         workbkgravity.loc[i,'RAC86-90']=sum(workbkrac.loc[workbkrac[i]==87.5,'rac'])
    #         workbkgravity.loc[i,'RAC91-95']=sum(workbkrac.loc[workbkrac[i]==92.5,'rac'])
    #         workbkgravity.loc[i,'RAC96-100']=sum(workbkrac.loc[workbkrac[i]==97.5,'rac'])
    #         workbkgravity.loc[i,'RAC101-105']=sum(workbkrac.loc[workbkrac[i]==102.5,'rac'])
    #         workbkgravity.loc[i,'RAC106-110']=sum(workbkrac.loc[workbkrac[i]==107.5,'rac'])
    #         workbkgravity.loc[i,'RAC111-115']=sum(workbkrac.loc[workbkrac[i]==112.5,'rac'])
    #         workbkgravity.loc[i,'RAC116-120']=sum(workbkrac.loc[workbkrac[i]==117.5,'rac'])
    #         workbkgravity.loc[i,'GRAC1-10']=(workbkgravity.loc[i,'RAC1-5']+workbkgravity.loc[i,'RAC6-10'])/(5**2)
    #         workbkgravity.loc[i,'GRAC11-20']=(workbkgravity.loc[i,'RAC11-15']+workbkgravity.loc[i,'RAC16-20'])/(15**2)
    #         workbkgravity.loc[i,'GRAC21-30']=(workbkgravity.loc[i,'RAC21-25']+workbkgravity.loc[i,'RAC26-30'])/(25**2)
    #         workbkgravity.loc[i,'GRAC31-40']=(workbkgravity.loc[i,'RAC31-35']+workbkgravity.loc[i,'RAC36-40'])/(35**2)
    #         workbkgravity.loc[i,'GRAC41-50']=(workbkgravity.loc[i,'RAC41-45']+workbkgravity.loc[i,'RAC46-50'])/(45**2)
    #         workbkgravity.loc[i,'GRAC51-60']=(workbkgravity.loc[i,'RAC51-55']+workbkgravity.loc[i,'RAC56-60'])/(55**2)
    #         workbkgravity.loc[i,'GRAVITYRAC']=workbkgravity.loc[i,'GRAC1-10']+workbkgravity.loc[i,'GRAC11-20']+workbkgravity.loc[i,'GRAC21-30']+workbkgravity.loc[i,'GRAC31-40']+workbkgravity.loc[i,'GRAC41-50']+workbkgravity.loc[i,'GRAC51-60']
    #     df=pd.concat([df,workbkgravity],axis=0)
    # df.to_csv(path+'ibx/topostgravity.csv',index=True)
        


    pre=pd.read_csv(path+'ibx/toprect.csv',dtype=float,converters={'tractid':str})
    pre=pd.melt(pre,id_vars='tractid',var_name='workct',value_name='time')
    pre.columns=['resct','workct','pre']
    post=pd.read_csv(path+'ibx/topostct.csv',dtype=float,converters={'tractid':str})
    post=pd.melt(post,id_vars='tractid',var_name='workct',value_name='time')
    post.columns=['resct','workct','pre']  
    df=pd.merge(pre,post,how='inner',on=['workct','resct'])
    df=df[~((df['pre']==999)&(df['post']==999))].reset_index(drop=True)
    df.to_csv(path+'ibx/ibxworkct.csv',index=False)
    
    



        
        
        
