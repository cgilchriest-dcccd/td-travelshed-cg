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
path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2018/TRAVELSHEDREVAMP/'
#path='C:/Users/Y_Ma2/Desktop/amazon/'
#doserver='http://142.93.21.138:8801/'
doserver='http://localhost:8801/'


# Block level summary
bmnbk=gpd.read_file(path+'shp/quadstatebkclipped.shp')
bmnbk.crs={'init': 'epsg:4326'}
bmnbk=bmnbk[['blockid','geometry']]    
bmnbk=bmnbk.set_index('blockid')

exwtbk=pd.read_csv(path+'bmn/exwtbk.csv',dtype=str)
exwtbk=exwtbk.set_index('blockid')
exwtbk.columns=[str(x).replace('BMN','')+'EXWT' for x in exwtbk.columns]
bmnbk=pd.concat([bmnbk,exwtbk],axis=1)

exprbk=pd.read_csv(path+'bmn/exprbk.csv',dtype=str)
exprbk=exprbk.set_index('blockid')
exprbk.columns=[str(x).replace('BMN','')+'EXPR' for x in exprbk.columns]
bmnbk=pd.concat([bmnbk,exprbk],axis=1)

exwtprbk=pd.read_csv(path+'bmn/exwtprbk.csv',dtype=str)
exwtprbk=exwtprbk.set_index('blockid')
exwtprbk.columns=[str(x).replace('BMN','')+'EXWTPR' for x in exwtprbk.columns]
bmnbk=pd.concat([bmnbk,exwtprbk],axis=1)

psaesawtbk=pd.read_csv(path+'bmn/psaesawtbk.csv',dtype=str)
psaesawtbk=psaesawtbk.set_index('blockid')
psaesawtbk.columns=[str(x).replace('BMN','')+'PSAWT' for x in psaesawtbk.columns]
bmnbk=pd.concat([bmnbk,psaesawtbk],axis=1)

psaesaprbk=pd.read_csv(path+'bmn/psaesaprbk.csv',dtype=str)
psaesaprbk=psaesaprbk.set_index('blockid')
psaesaprbk.columns=[str(x).replace('BMN','')+'PSAPR' for x in psaesaprbk.columns]
bmnbk=pd.concat([bmnbk,psaesaprbk],axis=1)

psaesawtprbk=pd.read_csv(path+'bmn/psaesawtprbk.csv',dtype=str)
psaesawtprbk=psaesawtprbk.set_index('blockid')
psaesawtprbk.columns=[str(x).replace('BMN','')+'PSAWTPR' for x in psaesawtprbk.columns]
bmnbk=pd.concat([bmnbk,psaesawtprbk],axis=1)

ssywtbk=pd.read_csv(path+'bmn/ssywtbk.csv',dtype=str)
ssywtbk=ssywtbk.set_index('blockid')
ssywtbk.columns=[str(x).replace('BMN','')+'SSYWT' for x in ssywtbk.columns]
bmnbk=pd.concat([bmnbk,ssywtbk],axis=1)

ssyprbk=pd.read_csv(path+'bmn/ssyprbk.csv',dtype=str)
ssyprbk=ssyprbk.set_index('blockid')
ssyprbk.columns=[str(x).replace('BMN','')+'SSYPR' for x in ssyprbk.columns]
bmnbk=pd.concat([bmnbk,ssyprbk],axis=1)

ssywtprbk=pd.read_csv(path+'bmn/ssywtprbk.csv',dtype=str)
ssywtprbk=ssywtprbk.set_index('blockid')
ssywtprbk.columns=[str(x).replace('BMN','')+'SSYWTPR' for x in ssywtprbk.columns]
bmnbk=pd.concat([bmnbk,ssywtprbk],axis=1)

for i in bmnbk.columns[1:]:
    bmnbk[i]=pd.to_numeric(bmnbk[i])
bmnbk=bmnbk.reset_index()
bmnbk.columns=['blockid']+list(bmnbk.columns[1:])
bmnbk.to_file(path+'bmn/bmnbk.shp')
bmnbk=bmnbk.drop('geometry',axis=1)
bmnbk.to_csv(path+'bmn/bmnbk.csv',index=False)



# Tract level summary
bmnct=gpd.read_file(path+'shp/quadstatectclipped.shp')
bmnct.crs={'init': 'epsg:4326'}
bmnct=bmnct[['tractid','geometry']]    
bmnct=bmnct.set_index('tractid')

exwtct=pd.read_csv(path+'bmn/exwtct.csv',dtype=str)
exwtct=exwtct.set_index('tractid')
exwtct.columns=[str(x).replace('BMN','')+'EXWT' for x in exwtct.columns]
bmnct=pd.concat([bmnct,exwtct],axis=1)

exprct=pd.read_csv(path+'bmn/exprct.csv',dtype=str)
exprct=exprct.set_index('tractid')
exprct.columns=[str(x).replace('BMN','')+'EXPR' for x in exprct.columns]
bmnct=pd.concat([bmnct,exprct],axis=1)

exwtprct=pd.read_csv(path+'bmn/exwtprct.csv',dtype=str)
exwtprct=exwtprct.set_index('tractid')
exwtprct.columns=[str(x).replace('BMN','')+'EXWTPR' for x in exwtprct.columns]
bmnct=pd.concat([bmnct,exwtprct],axis=1)

psaesawtct=pd.read_csv(path+'bmn/psaesawtct.csv',dtype=str)
psaesawtct=psaesawtct.set_index('tractid')
psaesawtct.columns=[str(x).replace('BMN','')+'PSAWT' for x in psaesawtct.columns]
bmnct=pd.concat([bmnct,psaesawtct],axis=1)

psaesaprct=pd.read_csv(path+'bmn/psaesaprct.csv',dtype=str)
psaesaprct=psaesaprct.set_index('tractid')
psaesaprct.columns=[str(x).replace('BMN','')+'PSAPR' for x in psaesaprct.columns]
bmnct=pd.concat([bmnct,psaesaprct],axis=1)

psaesawtprct=pd.read_csv(path+'bmn/psaesawtprct.csv',dtype=str)
psaesawtprct=psaesawtprct.set_index('tractid')
psaesawtprct.columns=[str(x).replace('BMN','')+'PSAWTPR' for x in psaesawtprct.columns]
bmnct=pd.concat([bmnct,psaesawtprct],axis=1)

ssywtct=pd.read_csv(path+'bmn/ssywtct.csv',dtype=str)
ssywtct=ssywtct.set_index('tractid')
ssywtct.columns=[str(x).replace('BMN','')+'SSYWT' for x in ssywtct.columns]
bmnct=pd.concat([bmnct,ssywtct],axis=1)

ssyprct=pd.read_csv(path+'bmn/ssyprct.csv',dtype=str)
ssyprct=ssyprct.set_index('tractid')
ssyprct.columns=[str(x).replace('BMN','')+'SSYPR' for x in ssyprct.columns]
bmnct=pd.concat([bmnct,ssyprct],axis=1)

ssywtprct=pd.read_csv(path+'bmn/ssywtprct.csv',dtype=str)
ssywtprct=ssywtprct.set_index('tractid')
ssywtprct.columns=[str(x).replace('BMN','')+'SSYWTPR' for x in ssywtprct.columns]
bmnct=pd.concat([bmnct,ssywtprct],axis=1)

for i in bmnct.columns[1:]:
    bmnct[i]=pd.to_numeric(bmnct[i])
bmnct=bmnct.reset_index()
bmnct.columns=['tractid']+list(bmnct.columns[1:])
bmnct.to_file(path+'bmn/bmnct.shp')
bmnct=bmnct.drop('geometry',axis=1)
bmnct.to_csv(path+'bmn/bmnct.csv',index=False)


