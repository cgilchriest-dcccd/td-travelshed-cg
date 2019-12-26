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
import matplotlib.pyplot as plt
import contextily as ctx
import mpl_toolkits.axes_grid1

start=datetime.datetime.now()

pd.set_option('display.max_columns', None)
path='/home/mayijun/TRAVELSHED/'
#path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2018/TRAVELSHEDREVAMP/'
#path='J:/TRAVELSHEDREVAMP/'
#path='E:/TRAVELSHED/'
#doserver='http://142.93.21.138:8801/'
doserver='http://localhost:8801/'



# Load quadstate block point shapefile
bkpt=gpd.read_file(path+'shp/quadstatebkpt.shp')
bkpt.crs={'init': 'epsg:4326'}

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
cutoffinterval=2 # in minutes
cutoffstart=0
cutoffend=120
cutoffincrement=cutoffstart
cutoff=''
while cutoffincrement<cutoffend:
    cutoff+='&cutoffSec='+str((cutoffincrement+cutoffinterval)*60)
    cutoffincrement+=cutoffinterval

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
    pool = mp.Pool(len(data))
    dt = pd.concat(pool.map(func, data),axis=1)
    pool.close()
    pool.join()
    return dt

## Define parallel multiprocessing function
#def parallelize(data, func):
#    data_split=np.array_split(data,np.ceil(len(data)/(mp.cpu_count()-1)))
#    pool=mp.Pool(mp.cpu_count()-1)
#    dt=pd.DataFrame()
#    for i in data_split:
#        ds=pd.concat(pool.map(func,i),axis=1)
#        dt=pd.concat([dt,ds],axis=1)
#    pool.close()
#    pool.join()
#    return dt

# Define add basemap
def add_basemap(ax, zoom, url='http://tile.stamen.com/terrain/tileZ/tileX/tileY.png'):
    xmin, xmax, ymin, ymax = ax.axis()
    basemap, extent = ctx.bounds2img(xmin, ymin, xmax, ymax, zoom=zoom, url=url)
    ax.imshow(basemap, extent=extent, interpolation='bilinear')
    ax.axis((xmin, xmax, ymin, ymax))



# Multiprocessing travelshed function for sites
if __name__=='__main__':
    location=pd.read_excel(path+'regionalrailnew/inbound/input2.xlsx',sheet_name='input',dtype=str)
    location['id']=location['siteid']
    location['latlong']=[str(x)+','+str(y) for x,y in zip(location['lat'],location['long'])]
    destination=location.loc[0:max(location.count())-1,['id','direction','stopname','latlong']]
#    destination=location.loc[[243,248,249,269,357],['id','direction','stopname','latlong']]
#    # Create travel time table for each site
#    for i in destination.index:
#        df=parallelize(arrivaltime,travelshedwt)
#        df['TTMEDIAN']=df.median(skipna=True,axis=1)
#        df=df['TTMEDIAN'].sort_index()
#        df.name=destination.loc[i,'id']
#        df.to_csv(path+'regionalrailnew/inbound/'+destination.loc[i,'id']+'wt.csv',index=True,header=True,na_rep=999)
    # Join travelsheds to block shapefile
    wtbk=gpd.read_file(path+'shp/quadstatebkclipped.shp')
    wtbk.crs={'init': 'epsg:4326'}
    wtbk=wtbk[['blockid','geometry']]    
    for i in destination.index:
        tp=pd.read_csv(path+'regionalrailnew/inbound/'+destination.loc[i,'id']+'wt.csv',dtype=str)
        tp.iloc[:,1]=pd.to_numeric(tp.iloc[:,1])
        wtbk=wtbk.merge(tp,on='blockid')
    wtbk.to_file(path+'regionalrailnew/inbound/wtbk.shp')
    wtbk=wtbk.drop('geometry',axis=1)
    wtbk.to_csv(path+'regionalrailnew/inbound/wtbk.csv',index=False)
    # Join travelsheds to tract shapefile
    wtbk=wtbk.replace(999,np.nan)
    loclist=wtbk.columns[1:]
    wtbk['tractid']=[str(x)[0:11] for x in wtbk['blockid']]
    wtbk=wtbk.groupby(['tractid'])[loclist].median(skipna=True)
    wtbk=wtbk.replace(np.nan,999)
    wtbk=wtbk.reset_index()
    wtct=gpd.read_file(path+'shp/quadstatectclipped.shp')
    wtct.crs={'init': 'epsg:4326'}
    wtct=wtct[['tractid','geometry']]
    wtct=wtct.merge(wtbk,on='tractid')
    wtct.to_file(path+'regionalrailnew/inbound/wtct.shp')
    wtct=wtct.drop('geometry',axis=1)
    wtct.to_csv(path+'regionalrailnew/inbound/wtct.csv',index=False)
    # Create map for each site
    wtbk=gpd.read_file(path+'regionalrailnew/inbound/wtbk.shp')
    wtbk.crs={'init': 'epsg:4326'}
    wtct=gpd.read_file(path+'regionalrailnew/inbound/wtct.shp')
    wtct.crs={'init': 'epsg:4326'}
    for i in destination.index:
        # Create block level map
        wtbkmap=wtbk.loc[wtbk[destination.loc[i,'id']]<=90,[destination.loc[i,'id'],'geometry']]
        wtbkmap=wtbkmap.to_crs(epsg=3857)
        fig,ax=plt.subplots(1,figsize=(11,8.5))
        plt.subplots_adjust(left=0.05,right=0.95,top=0.95,bottom=0.05)
        ax=wtbkmap.plot(figsize=(11,8.5),edgecolor=None,column=destination.loc[i,'id'],cmap='Spectral',linewidth=0.2,ax=ax,alpha=0.7)
        add_basemap(ax,zoom=11,url=ctx.sources.ST_TONER_LITE)
        ax.set_axis_off()
        if destination.loc[i,'direction']=='in':
            ax.set_title('AM Peak Transit Travel Time (Minutes) to '+destination.loc[i,'stopname'],fontdict={'fontsize':'16','fontweight':'10'})
        elif destination.loc[i,'direction']=='out':
            ax.set_title('AM Peak Transit Travel Time (Minutes) from '+destination.loc[i,'stopname'],fontdict={'fontsize':'16','fontweight':'10'})
        sm=plt.cm.ScalarMappable(cmap='Spectral',norm=plt.Normalize(vmin=1,vmax=90))
        sm._A=[]
        divider=mpl_toolkits.axes_grid1.make_axes_locatable(ax)
        cax=divider.append_axes("right",size="3%",pad=0.2,aspect=25)
        cbar=fig.colorbar(sm,cax=cax)
        fig.tight_layout()
        fig.savefig(path+'regionalrailnew/inbound/'+destination.loc[i,'id']+'wtbk.jpg', dpi=300)
        # Create tract level map
        wtctmap=wtct.loc[wtct[destination.loc[i,'id']]<=90,[destination.loc[i,'id'],'geometry']]
        wtctmap=wtctmap.to_crs(epsg=3857)
        fig,ax=plt.subplots(1,figsize=(11,8.5))
        plt.subplots_adjust(left=0.05,right=0.95,top=0.95,bottom=0.05)
        ax=wtctmap.plot(figsize=(11,8.5),edgecolor=None,column=destination.loc[i,'id'],cmap='Spectral',linewidth=0.2,ax=ax,alpha=0.7)
        add_basemap(ax,zoom=11,url=ctx.sources.ST_TONER_LITE)
        ax.set_axis_off()
        if destination.loc[i,'direction']=='in':
            ax.set_title('AM Peak Transit Travel Time (Minutes) to '+destination.loc[i,'stopname'],fontdict={'fontsize':'16','fontweight':'10'})
        elif destination.loc[i,'direction']=='out':
            ax.set_title('AM Peak Transit Travel Time (Minutes) from '+destination.loc[i,'stopname'],fontdict={'fontsize':'16','fontweight':'10'})
        sm=plt.cm.ScalarMappable(cmap='Spectral',norm=plt.Normalize(vmin=1,vmax=90))
        sm._A=[]
        divider=mpl_toolkits.axes_grid1.make_axes_locatable(ax)
        cax=divider.append_axes("right",size="3%",pad=0.2,aspect=25)
        cbar=fig.colorbar(sm,cax=cax)
        fig.tight_layout()
        fig.savefig(path+'regionalrailnew/inbound/'+destination.loc[i,'id']+'wtct.jpg', dpi=300)
    print(datetime.datetime.now()-start)