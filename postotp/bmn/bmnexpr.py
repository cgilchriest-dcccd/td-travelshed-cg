#! /usr/bin/python3
# Travelshed Mapping
# OpenTripPlanner Guide: http://docs.opentripplanner.org/en/latest/
# OpenTripPlanner API Doc: http://dev.opentripplanner.org/apidoc/1.0.0/index.html

import matplotlib
matplotlib.use('Agg')

import datetime
import geopandas as gpd
import pandas as pd
import numpy as np
import requests
import multiprocessing as mp
import matplotlib.pyplot as plt
import contextily as ctx
import mpl_toolkits.axes_grid1

start=datetime.datetime.now()

pd.set_option('display.max_columns', None)
path='/home/mayijun/TRAVELSHED/'
#path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2018/TRAVELSHEDREVAMP/'
#path='C:/Users/Y_Ma2/Desktop/amazon/'
#doserver='http://142.93.21.138:8801/'
doserver='http://localhost:8801/'
#nyc=['36005','36047','36061','36081','36085']
nyc=[]


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

# Set maximum pre transit free flow driving time
maxPreTransitTime=10 # in minutes

# Set cut off points between 0-120 mins
cutoffinterval=2 # in minutes
cutoffstart=0
cutoffend=120
cutoffincrement=cutoffstart
cutoff=''
while cutoffincrement<cutoffend:
    cutoff+='&cutoffSec='+str((cutoffincrement+cutoffinterval)*60)
    cutoffincrement+=cutoffinterval

# Penalty for traffic and parking time
penalty=5 # in minutes

# Definie res travelshed function to generate isochrones and spatial join to Census Blocks
def travelshedpr(arrt):
    bk=bkpt.copy()
    if destination.loc[i,'direction']=='in':
        url='http://localhost:8801/otp/routers/default/isochrone?batch=true&mode=CAR,WALK,TRANSIT'
        url+='&fromPlace='+destination.loc[i,'latlong']+'&toPlace='+destination.loc[i,'latlong']
        url+='&arriveBy=true&date='+typicaldate+'&time='+arrt+'&maxTransfers='+str(maxTransfers)
        url+='&maxWalkDistance='+str(maxWalkDistance)+'&maxPreTransitTime='+str(maxPreTransitTime*60)+'&clampInitialWait=-1'+cutoff
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
                bk.loc[bk['blockid'].isin(bkiso),'T'+arrt[0:2]+arrt[3:5]]=cut[0]-cutoffinterval/2+penalty
            except ValueError:
                print(destination.loc[i,'id']+' '+arrt+' '+
                      str(cut[0])+'-minute isochrone has no Census Block in it!')
            for k in range(0,(len(cut)-1)):
                if (iso.loc[iso['time']==cut[k+1]*60,'geometry'].notna()).bool():
                    if len(bk.loc[bk['T'+arrt[0:2]+arrt[3:5]]==cut[k]-cutoffinterval/2+penalty])!=0:
                        try:
                            bkiso=gpd.sjoin(bk.loc[bk['T'+arrt[0:2]+arrt[3:5]]==cut[k]-cutoffinterval/2+penalty],
                                            iso.loc[iso['time']==cut[k+1]*60],how='left',op='within')
                            bkiso=bkiso.loc[pd.notnull(bkiso['time']),'blockid']
                            bk.loc[bk['blockid'].isin(bkiso),'T'+arrt[0:2]+arrt[3:5]]=cut[k+1]-cutoffinterval/2+penalty
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
        bk['T'+arrt[0:2]+arrt[3:5]]=bk['T'+arrt[0:2]+arrt[3:5]].replace(124,np.nan)
        bk['T'+arrt[0:2]+arrt[3:5]]=bk['T'+arrt[0:2]+arrt[3:5]].replace(122,np.nan)
        bk=bk.drop(['lat','long','geometry'],axis=1)
        bk=bk.set_index('blockid')
        return bk
    elif destination.loc[i,'direction']=='out':
        url='http://localhost:8801/otp/routers/default/isochrone?batch=true&mode=CAR,WALK,TRANSIT'
        url+='&fromPlace='+destination.loc[i,'latlong']
        url+='&date='+typicaldate+'&time='+arrt+'&maxTransfers='+str(maxTransfers)
        url+='&maxWalkDistance='+str(maxWalkDistance)+'&maxPreTransitTime='+str(maxPreTransitTime*60)+'&clampInitialWait=-1'+cutoff
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
                bk.loc[bk['blockid'].isin(bkiso),'T'+arrt[0:2]+arrt[3:5]]=cut[0]-cutoffinterval/2+penalty
            except ValueError:
                print(destination.loc[i,'id']+' '+arrt+' '+
                      str(cut[0])+'-minute isochrone has no Census Block in it!')
            for k in range(0,(len(cut)-1)):
                if (iso.loc[iso['time']==cut[k+1]*60,'geometry'].notna()).bool():
                    if len(bk.loc[bk['T'+arrt[0:2]+arrt[3:5]]==cut[k]-cutoffinterval/2+penalty])!=0:
                        try:
                            bkiso=gpd.sjoin(bk.loc[bk['T'+arrt[0:2]+arrt[3:5]]==cut[k]-cutoffinterval/2+penalty],
                                            iso.loc[iso['time']==cut[k+1]*60],how='left',op='within')
                            bkiso=bkiso.loc[pd.notnull(bkiso['time']),'blockid']
                            bk.loc[bk['blockid'].isin(bkiso),'T'+arrt[0:2]+arrt[3:5]]=cut[k+1]-cutoffinterval/2+penalty
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
        bk['T'+arrt[0:2]+arrt[3:5]]=bk['T'+arrt[0:2]+arrt[3:5]].replace(124,np.nan)
        bk['T'+arrt[0:2]+arrt[3:5]]=bk['T'+arrt[0:2]+arrt[3:5]].replace(122,np.nan)
        bk=bk.drop(['lat','long','geometry'],axis=1)
        bk=bk.set_index('blockid')
        return bk


# Define parallel multiprocessing function
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

# Define add basemap
def add_basemap(ax, zoom, url='http://tile.stamen.com/terrain/tileZ/tileX/tileY.png'):
    xmin, xmax, ymin, ymax = ax.axis()
    basemap, extent = ctx.bounds2img(xmin, ymin, xmax, ymax, zoom=zoom, url=url)
    ax.imshow(basemap, extent=extent, interpolation='bilinear')
    ax.axis((xmin, xmax, ymin, ymax))



# Multiprocessing travelshed function for sites
if __name__=='__main__':
    location=pd.read_excel(path+'bmn/input.xlsx',sheet_name='input',dtype=str)
    location['id']=location['siteid']
    location['latlong']=[str(x)+','+str(y) for x,y in zip(location['intlat'],location['intlong'])]
    destination=location.loc[0:max(location.count())-1,['id','direction','name','latlong']]
#    # Create travel time table for each site
#    for i in destination.index:
#        df=parallelize(arrivaltime,travelshedpr)
#        df['TTMEDIAN']=df.median(skipna=True,axis=1)
#        df=df['TTMEDIAN'].sort_index()
#        df.name=destination.loc[i,'id']
#        df.to_csv(path+'bmn/'+destination.loc[i,'id']+'expr.csv',index=True,header=True,na_rep=999)
    # Join park and ride travelsheds to block shapefiles
    prbk=gpd.read_file(path+'shp/quadstatebkclipped.shp')
    prbk.crs={'init': 'epsg:4326'}
    prbk=prbk[['blockid','geometry']]  
    for i in destination.index:
        tp=pd.read_csv(path+'bmn/'+destination.loc[i,'id']+'expr.csv',dtype=str)
        tp.iloc[:,1]=pd.to_numeric(tp.iloc[:,1])
        prbk=prbk.merge(tp,on='blockid')
    prbk.to_file(path+'bmn/exprbk.shp')
    prbk=prbk.drop('geometry',axis=1)
    prbk.to_csv(path+'bmn/exprbk.csv',index=False)
    # Join park and ride travelsheds to tract shapefile
    prbk=prbk.replace(999,np.nan)
    loclist=list(prbk.columns[1:])
    prbk['tractid']=[str(x)[0:11] for x in prbk['blockid']]
    prbk=prbk.groupby(['tractid'])[loclist].median(skipna=True)
    prbk=prbk.replace(np.nan,999)
    prbk=prbk.reset_index()
    prct=gpd.read_file(path+'shp/quadstatectclipped.shp')
    prct.crs={'init': 'epsg:4326'}
    prct=prct[['tractid','geometry']]
    prct=prct.merge(prbk,on='tractid')
    prct.to_file(path+'bmn/exprct.shp')
    prct=prct.drop('geometry',axis=1)
    prct.to_csv(path+'bmn/exprct.csv',index=False)    
    # Create park and ride map for each site
    prbk=gpd.read_file(path+'bmn/exprbk.shp')
    prbk.crs={'init': 'epsg:4326'}
    prct=gpd.read_file(path+'bmn/exprct.shp')
    prct.crs={'init': 'epsg:4326'}
    for i in destination.index:
        # Create block level map
        prbkmap=prbk.loc[prbk[destination.loc[i,'id']]<=90,[destination.loc[i,'id'],'geometry']]
        prbkmap=prbkmap.to_crs(epsg=3857)
        fig,ax=plt.subplots(1,figsize=(11,8.5))
        plt.subplots_adjust(left=0.05,right=0.95,top=0.95,bottom=0.05)
        ax=prbkmap.plot(figsize=(11,8.5),edgecolor=None,column=destination.loc[i,'id'],cmap='Spectral',linewidth=0.2,ax=ax,alpha=0.7)
        add_basemap(ax,zoom=11,url=ctx.sources.ST_TONER_LITE)
        ax.set_axis_off()
        if destination.loc[i,'direction']=='in':
            ax.set_title('AM Peak Transit Travel Time (Minutes) to '+destination.loc[i,'name'],fontdict={'fontsize':'16','fontweight':'10'})
        elif destination.loc[i,'direction']=='out':
            ax.set_title('AM Peak Transit Travel Time (Minutes) from '+destination.loc[i,'name'],fontdict={'fontsize':'16','fontweight':'10'})
        sm=plt.cm.ScalarMappable(cmap='Spectral',norm=plt.Normalize(vmin=1,vmax=90))
        sm._A=[]
        divider=mpl_toolkits.axes_grid1.make_axes_locatable(ax)
        cax=divider.append_axes("right",size="3%",pad=0.2,aspect=25)
        cbar=fig.colorbar(sm,cax=cax)
        fig.tight_layout()
        fig.savefig(path+'bmn/'+destination.loc[i,'id']+'exprbk.jpg', dpi=300)
        # Create tract level map
        prbkmap=prct.loc[prct[destination.loc[i,'id']]<=90,[destination.loc[i,'id'],'geometry']]
        prbkmap=prbkmap.to_crs(epsg=3857)
        fig,ax=plt.subplots(1,figsize=(11,8.5))
        plt.subplots_adjust(left=0.05,right=0.95,top=0.95,bottom=0.05)
        ax=prbkmap.plot(figsize=(11,8.5),edgecolor=None,column=destination.loc[i,'id'],cmap='Spectral',linewidth=0.2,ax=ax,alpha=0.7)
        add_basemap(ax,zoom=11,url=ctx.sources.ST_TONER_LITE)
        ax.set_axis_off()
        if destination.loc[i,'direction']=='in':
            ax.set_title('AM Peak Transit Travel Time (Minutes) to '+destination.loc[i,'name'],fontdict={'fontsize':'16','fontweight':'10'})
        elif destination.loc[i,'direction']=='out':
            ax.set_title('AM Peak Transit Travel Time (Minutes) from '+destination.loc[i,'name'],fontdict={'fontsize':'16','fontweight':'10'})
        sm=plt.cm.ScalarMappable(cmap='Spectral',norm=plt.Normalize(vmin=1,vmax=90))
        sm._A=[]
        divider=mpl_toolkits.axes_grid1.make_axes_locatable(ax)
        cax=divider.append_axes("right",size="3%",pad=0.2,aspect=25)
        cbar=fig.colorbar(sm,cax=cax)
        fig.tight_layout()
        fig.savefig(path+'bmn/'+destination.loc[i,'id']+'exprct.jpg', dpi=300)
    # Join walk and transit + park and ride travelsheds to block shapefile
    wtbk=pd.read_csv(path+'bmn/exwtbk.csv',dtype=str)
    wtbk=wtbk.set_index('blockid')
    wtbk.columns=['WT'+str(x) for x in wtbk.columns]
    wtbk=wtbk.reset_index()
    prbk=pd.read_csv(path+'bmn/exprbk.csv',dtype=str)
    prbk=prbk.set_index('blockid')
    prbk.columns=['PR'+str(x) for x in prbk.columns]
    prbk=prbk.reset_index()
    wtprbk=gpd.read_file(path+'shp/quadstatebkclipped.shp')
    wtprbk.crs={'init': 'epsg:4326'}
    wtprbk=wtprbk[['blockid','geometry']]
    wtprbk=wtprbk.merge(wtbk,on='blockid')
    wtprbk=wtprbk.merge(prbk,on='blockid')
    for i in wtprbk.columns[2:]:
        wtprbk[i]=pd.to_numeric(wtprbk[i])
    for i in loclist:
        wtprbk[i]=np.where([str(x)[0:5] not in nyc for x in wtprbk['blockid']],
                           pd.concat([wtprbk['WT'+str(i)],wtprbk['PR'+str(i)]],axis=1).min(axis=1),
                           wtprbk['WT'+str(i)])
    wtprbk=wtprbk[['blockid']+loclist+['geometry']]
    wtprbk.to_file(path+'bmn/exwtprbk.shp')
    wtprbk=wtprbk.drop('geometry',axis=1)
    wtprbk.to_csv(path+'bmn/exwtprbk.csv',index=False)
    # Join walk and transit + park and ride travelsheds to tract shapefile
    wtprbk=wtprbk.replace(999,np.nan)
    wtprbk['tractid']=[str(x)[0:11] for x in wtprbk['blockid']]
    wtprbk=wtprbk.groupby(['tractid'])[loclist].median(skipna=True)
    wtprbk=wtprbk.replace(np.nan,999)
    wtprbk=wtprbk.reset_index()
    wtprct=gpd.read_file(path+'shp/quadstatectclipped.shp')
    wtprct.crs={'init': 'epsg:4326'}
    wtprct=wtprct[['tractid','geometry']]
    wtprct=wtprct.merge(wtprbk,on='tractid')
    wtprct.to_file(path+'bmn/exwtprct.shp')
    wtprct=wtprct.drop('geometry',axis=1)
    wtprct.to_csv(path+'bmn/exwtprct.csv',index=False)
    # Create walk and transit + park and ride map for each site
    wtprbk=gpd.read_file(path+'bmn/exwtprbk.shp')
    wtprbk.crs={'init': 'epsg:4326'}
    wtprct=gpd.read_file(path+'bmn/exwtprct.shp')
    wtprct.crs={'init': 'epsg:4326'}
    for i in destination.index:
        # Create block level map
        wtprbkmap=wtprbk.loc[wtprbk[destination.loc[i,'id']]<=90,[destination.loc[i,'id'],'geometry']]
        wtprbkmap=wtprbkmap.to_crs(epsg=3857)
        fig,ax=plt.subplots(1,figsize=(11,8.5))
        plt.subplots_adjust(left=0.05,right=0.95,top=0.95,bottom=0.05)
        ax=wtprbkmap.plot(figsize=(11,8.5),edgecolor=None,column=destination.loc[i,'id'],cmap='Spectral',linewidth=0.2,ax=ax,alpha=0.7)
        add_basemap(ax,zoom=11,url=ctx.sources.ST_TONER_LITE)
        ax.set_axis_off()
        if destination.loc[i,'direction']=='in':
            ax.set_title('AM Peak Transit Travel Time (Minutes) to '+destination.loc[i,'name'],fontdict={'fontsize':'16','fontweight':'10'})
        elif destination.loc[i,'direction']=='out':
            ax.set_title('AM Peak Transit Travel Time (Minutes) from '+destination.loc[i,'name'],fontdict={'fontsize':'16','fontweight':'10'})
        sm=plt.cm.ScalarMappable(cmap='Spectral',norm=plt.Normalize(vmin=1,vmax=90))
        sm._A=[]
        divider=mpl_toolkits.axes_grid1.make_axes_locatable(ax)
        cax=divider.append_axes("right",size="3%",pad=0.2,aspect=25)
        cbar=fig.colorbar(sm,cax=cax)
        fig.tight_layout()
        fig.savefig(path+'bmn/'+destination.loc[i,'id']+'exwtprbk.jpg', dpi=300)
        # Create tract level map
        wtprbkmap=wtprct.loc[wtprct[destination.loc[i,'id']]<=90,[destination.loc[i,'id'],'geometry']]
        wtprbkmap=wtprbkmap.to_crs(epsg=3857)
        fig,ax=plt.subplots(1,figsize=(11,8.5))
        plt.subplots_adjust(left=0.05,right=0.95,top=0.95,bottom=0.05)
        ax=wtprbkmap.plot(figsize=(11,8.5),edgecolor=None,column=destination.loc[i,'id'],cmap='Spectral',linewidth=0.2,ax=ax,alpha=0.7)
        add_basemap(ax,zoom=11,url=ctx.sources.ST_TONER_LITE)
        ax.set_axis_off()
        if destination.loc[i,'direction']=='in':
            ax.set_title('AM Peak Transit Travel Time (Minutes) to '+destination.loc[i,'name'],fontdict={'fontsize':'16','fontweight':'10'})
        elif destination.loc[i,'direction']=='out':
            ax.set_title('AM Peak Transit Travel Time (Minutes) from '+destination.loc[i,'name'],fontdict={'fontsize':'16','fontweight':'10'})
        sm=plt.cm.ScalarMappable(cmap='Spectral',norm=plt.Normalize(vmin=1,vmax=90))
        sm._A=[]
        divider=mpl_toolkits.axes_grid1.make_axes_locatable(ax)
        cax=divider.append_axes("right",size="3%",pad=0.2,aspect=25)
        cbar=fig.colorbar(sm,cax=cax)
        fig.tight_layout()
        fig.savefig(path+'bmn/'+destination.loc[i,'id']+'exwtprct.jpg', dpi=300)
    print(datetime.datetime.now()-start)








