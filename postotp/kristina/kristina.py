#! /usr/bin/python3


import geopandas as gpd
import pandas as pd
import numpy as np

pd.set_option('display.max_columns', 500)
pd.set_option('display.max_colwidth',500)
pd.set_option('display.max_rows',500)

#path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2018/TRAVELSHEDREVAMP/'
#path='/home/mayijun/TRAVELSHED/'
path='E:/TRAVELSHEDREVAMP/'
nyc=['36005','36047','36061','36081','36085']


# tract
# allstation
df=pd.read_csv(path+'kristina/travelshedct3.csv',dtype=str)
kristina=df[['tractid','S611']]
kristina['S611']=pd.to_numeric(kristina['S611'])

# kristina
ct=gpd.read_file(path+'shp/quadstatectclipped.shp')
ct.crs={'init': 'epsg:4326'}
ct=ct[['tractid','geometry']]

ct=ct.merge(kristina,on='tractid')
ct=ct.iloc[[x[0:5] in nyc for x in ct['tractid']],:]
ct.to_file(path+'kristina/kristinact.shp')





# block
# allstation
df=pd.read_csv(path+'kristina/travelshedbk3.csv',dtype=str)
kristina=df[['blockid','S611']]
kristina['S611']=pd.to_numeric(kristina['S611'])

# kristina
bk=gpd.read_file(path+'shp/quadstatebkclipped.shp')
bk.crs={'init': 'epsg:4326'}
bk=bk[['blockid','geometry']]

bk=bk.merge(kristina,on='blockid')
bk=bk.iloc[[x[0:5] in nyc for x in bk['blockid']],:]
bk.to_file(path+'kristina/kristinabk.shp')





