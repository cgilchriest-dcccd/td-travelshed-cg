#! /usr/bin/python3
# Travelshed Mapping
# OpenTripPlanner Guide: http://docs.opentripplanner.org/en/latest/
# OpenTripPlanner API Doc: http://dev.opentripplanner.org/apidoc/1.0.0/index.html

import matplotlib

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
#path='C:/Users/Y_Ma2/Desktop/amazon/'
#path='J:/TRAVELSHEDREVAMP/'
#doserver='http://142.93.21.138:8801/'
doserver='http://localhost:8801/'

#
## Define add basemap
#def add_basemap(ax, zoom, url='http://tile.stamen.com/terrain/tileZ/tileX/tileY.png'):
#    xmin, xmax, ymin, ymax = ax.axis()
#    basemap, extent = ctx.bounds2img(xmin, ymin, xmax, ymax, zoom=zoom, url=url)
#    ax.imshow(basemap, extent=extent, interpolation='bilinear')
#    ax.axis((xmin, xmax, ymin, ymax))
#
#
#
## Map adjusted Walk and Transit
#ct=gpd.read_file(path+'shp/quadstatectclipped.shp')
#ct.crs={'init': 'epsg:4326'}
#ct=ct[['tractid','geometry']]    
#wtct=pd.read_csv(path+'allstation/wt/wtct2.csv',dtype=str)
#for i in wtct.columns[1:]:
#    wtct[i]=pd.to_numeric(wtct[i])
#wtct=ct.merge(wtct,on='tractid')
#for i in ['SUBWAY197']:
#    wtctmap=wtct.loc[wtct[i]<=120,[i,'geometry']]
#    wtctmap=wtctmap.to_crs(epsg=3857)
#    fig,ax=plt.subplots(1,figsize=(11,8.5))
#    plt.subplots_adjust(left=0.05,right=0.95,top=0.95,bottom=0.05)
#    ax=wtctmap.plot(figsize=(11,8.5),edgecolor=None,column=i,cmap='Spectral',linewidth=0.2,ax=ax,alpha=0.7)
#    add_basemap(ax,zoom=11,url=ctx.sources.ST_TONER_LITE)
#    ax.set_axis_off()
#    sm=plt.cm.ScalarMappable(cmap='Spectral',norm=plt.Normalize(vmin=1,vmax=90))
#    sm._A=[]
#    divider=mpl_toolkits.axes_grid1.make_axes_locatable(ax)
#    cax=divider.append_axes("right",size="3%",pad=0.2,aspect=25)
#    cbar=fig.colorbar(sm,cax=cax)
#    fig.tight_layout()
#    fig.savefig(path+'allstation/wt/ADJ'+i+'wtct.jpg', dpi=300)
#
## Map adjusted Park and Ride
#ct=gpd.read_file(path+'shp/quadstatectclipped.shp')
#ct.crs={'init': 'epsg:4326'}
#ct=ct[['tractid','geometry']]    
#prct=pd.read_csv(path+'allstation/pr/prct2.csv',dtype=str)
#for i in prct.columns[1:]:
#    prct[i]=pd.to_numeric(prct[i])
#prct=ct.merge(prct,on='tractid')
#for i in ['SUBWAY197']:
#    prctmap=prct.loc[prct[i]<=120,[i,'geometry']]
#    prctmap=prctmap.to_crs(epsg=3857)
#    fig,ax=plt.subplots(1,figsize=(11,8.5))
#    plt.subplots_adjust(left=0.05,right=0.95,top=0.95,bottom=0.05)
#    ax=prctmap.plot(figsize=(11,8.5),edgecolor=None,column=i,cmap='Spectral',linewidth=0.2,ax=ax,alpha=0.7)
#    add_basemap(ax,zoom=11,url=ctx.sources.ST_TONER_LITE)
#    ax.set_axis_off()
#    sm=plt.cm.ScalarMappable(cmap='Spectral',norm=plt.Normalize(vmin=1,vmax=90))
#    sm._A=[]
#    divider=mpl_toolkits.axes_grid1.make_axes_locatable(ax)
#    cax=divider.append_axes("right",size="3%",pad=0.2,aspect=25)
#    cbar=fig.colorbar(sm,cax=cax)
#    fig.tight_layout()
#    fig.savefig(path+'allstation/pr/ADJ'+i+'prct.jpg', dpi=300)
#
## Map adjusted Walk and Transit + Park and Ride
#ct=gpd.read_file(path+'shp/quadstatectclipped.shp')
#ct.crs={'init': 'epsg:4326'}
#ct=ct[['tractid','geometry']]    
#tsct=pd.read_csv(path+'allstation/travelshedct2.csv',dtype=str)
#for i in tsct.columns[1:]:
#    tsct[i]=pd.to_numeric(tsct[i])
#tsct=ct.merge(tsct,on='tractid')
#for i in ['SUBWAY197']:
#    tsctmap=tsct.loc[tsct[i]<=120,[i,'geometry']]
#    tsctmap=tsctmap.to_crs(epsg=3857)
#    fig,ax=plt.subplots(1,figsize=(11,8.5))
#    plt.subplots_adjust(left=0.05,right=0.95,top=0.95,bottom=0.05)
#    ax=tsctmap.plot(figsize=(11,8.5),edgecolor=None,column=i,cmap='Spectral',linewidth=0.2,ax=ax,alpha=0.7)
#    add_basemap(ax,zoom=11,url=ctx.sources.ST_TONER_LITE)
#    ax.set_axis_off()
#    sm=plt.cm.ScalarMappable(cmap='Spectral',norm=plt.Normalize(vmin=1,vmax=90))
#    sm._A=[]
#    divider=mpl_toolkits.axes_grid1.make_axes_locatable(ax)
#    cax=divider.append_axes("right",size="3%",pad=0.2,aspect=25)
#    cbar=fig.colorbar(sm,cax=cax)
#    fig.tight_layout()
#    fig.savefig(path+'allstation/ADJ'+i+'prct.jpg', dpi=300)





# Change id
location=pd.read_excel(path+'allstation/inbound/location.xlsx',sheet_name='location',dtype=str)

wtbk=pd.read_csv(path+'allstation/inbound/wt/wtbk2.csv',dtype=str)
loclist=pd.DataFrame(wtbk.columns[1:])
loclist=loclist.merge(location,left_on=0,right_on='locationid',how='left')
loclist=list(loclist['newlocationid'])
wtbk.columns=['blockid']+loclist
wtbk.to_csv(path+'allstation/inbound/wt/wtbk3.csv',index=False,header=True)

wtct=pd.read_csv(path+'allstation/inbound/wt/wtct2.csv',dtype=str)
loclist=pd.DataFrame(wtct.columns[1:])
loclist=loclist.merge(location,left_on=0,right_on='locationid',how='left')
loclist=list(loclist['newlocationid'])
wtct.columns=['tractid']+loclist
wtct.to_csv(path+'allstation/inbound/wt/wtct3.csv',index=False,header=True)

prbk=pd.read_csv(path+'allstation/inbound/pr/prbk2.csv',dtype=str)
loclist=pd.DataFrame(prbk.columns[1:])
loclist=loclist.merge(location,left_on=0,right_on='locationid',how='left')
loclist=list(loclist['newlocationid'])
prbk.columns=['blockid']+loclist
prbk.to_csv(path+'allstation/inbound/pr/prbk3.csv',index=False,header=True)

prct=pd.read_csv(path+'allstation/inbound/pr/prct2.csv',dtype=str)
loclist=pd.DataFrame(prct.columns[1:])
loclist=loclist.merge(location,left_on=0,right_on='locationid',how='left')
loclist=list(loclist['newlocationid'])
prct.columns=['tractid']+loclist
prct.to_csv(path+'allstation/inbound/pr/prct3.csv',index=False,header=True)

tsbk=pd.read_csv(path+'allstation/inbound/travelshedbk2.csv',dtype=str)
loclist=pd.DataFrame(tsbk.columns[1:])
loclist=loclist.merge(location,left_on=0,right_on='locationid',how='left')
loclist=list(loclist['newlocationid'])
tsbk.columns=['blockid']+loclist
tsbk.to_csv(path+'allstation/inbound/travelshedbk3.csv',index=False,header=True)

tsct=pd.read_csv(path+'allstation/inbound/travelshedct2.csv',dtype=str)
loclist=pd.DataFrame(tsct.columns[1:])
loclist=loclist.merge(location,left_on=0,right_on='locationid',how='left')
loclist=list(loclist['newlocationid'])
tsct.columns=['tractid']+loclist
tsct.to_csv(path+'allstation/inbound/travelshedct3.csv',index=False,header=True)

gravitybk=pd.read_csv(path+'allstation/inbound/gravitybk2.csv',dtype=str)
loclist=pd.DataFrame(gravitybk['Unnamed: 0'])
loclist=loclist.merge(location,left_on='Unnamed: 0',right_on='locationid',how='left')
loclist=list(loclist['newlocationid'])
gravitybk['Unnamed: 0']=loclist
gravitybk.to_csv(path+'allstation/inbound/gravitybk3.csv',index=False,header=True)
