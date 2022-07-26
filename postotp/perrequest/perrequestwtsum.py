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
# path='/home/mayijun/TRAVELSHED/'
path='C:/Users/mayij/Desktop/DOC/DCP2018/TRAVELSHEDREVAMP/'
doserver='http://159.65.64.166:8801/'
# doserver='http://localhost:8801/'



start=datetime.datetime.now()

# Summarize all sites and create images
if __name__=='__main__':
    location=pd.read_excel(path+'waterfront2/input.xlsx',sheet_name='input',dtype=str)
    location['id']=['SITE'+str(x).zfill(4) for x in location['siteid']]
    location['latlong']=[str(x)+','+str(y) for x,y in zip(location['lat'],location['long'])]
    destination=location.loc[0:max(location.count())-1,['id','direction','latlong']].reset_index(drop=True)

    # Join site travelsheds to block shapefile
    wtbk=gpd.read_file(path+'shp/quadstatebkclipped.shp')
    wtbk.crs=4326
    wtbk=wtbk.drop(['lat','long'],axis=1)
    for i in destination.index:
        tp=pd.read_csv(path+'perrequest/'+destination.loc[i,'id']+'wt.csv',dtype=float,converters={'blockid':str})
        wtbk=wtbk.merge(tp,on='blockid')
    wtbk.to_file(path+'perrequest/wtbk.shp')
    wtbk=wtbk.drop('geometry',axis=1)
    wtbk.to_csv(path+'perrequest/wtbk.csv',index=False)
    # Join site travelsheds to tract shapefile
    wtbk=wtbk.replace(999,np.nan)
    loclist=wtbk.columns[1:]
    wtbk['tractid']=[str(x)[0:11] for x in wtbk['blockid']]
    wtbk=wtbk.groupby(['tractid'],as_index=False)[loclist].median()
    wtbk=wtbk.replace(np.nan,999)
    wtct=gpd.read_file(path+'shp/quadstatectclipped.shp')
    wtct.crs=4326
    wtct=wtct.drop(['lat','long'],axis=1)
    wtct=wtct.merge(wtbk,on='tractid')
    wtct.to_file(path+'perrequest/wtct.shp')
    wtct=wtct.drop('geometry',axis=1)
    wtct.to_csv(path+'perrequest/wtct.csv',index=False)
    print(datetime.datetime.now()-start)

