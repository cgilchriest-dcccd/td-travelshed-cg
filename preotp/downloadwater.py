#! /usr/bin/python3
# Travelshed Mapping
# OpenTripPlanner Guide: http://docs.opentripplanner.org/en/latest/
# OpenTripPlanner API Doc: http://dev.opentripplanner.org/apidoc/1.0.0/index.html


import urllib.request
import shutil
import zipfile
import geopandas as gpd
import pandas as pd
import time


pd.set_option('display.max_columns', None)
#path='/home/mayijun/TRAVELSHED/'
path='C:/Users/mayij/Desktop/DOC/DCP2018/TRAVELSHEDREVAMP/'


quadstatecounty=['09'+str(x).zfill(3) for x in range(1,17,2)]
quadstatecounty.extend(['34'+str(x).zfill(3) for x in range(1,43,2)])
quadstatecounty.extend(['36'+str(x).zfill(3) for x in range(1,125,2)])
quadstatecounty.extend(['42'+str(x).zfill(3) for x in range(1,135,2)])


for i in quadstatecounty:
    url='https://www2.census.gov/geo/tiger/TIGER2021/AREAWATER/tl_2021_'+str(i)+'_areawater.zip'
    req=urllib.request.urlopen(url)
    file=open(path+'water/download/tl_2021_'+str(i)+'_areawater.zip', "wb")
    shutil.copyfileobj(req,file)
    file.close()
    zip_ref=zipfile.ZipFile(path+'water/download/tl_2021_'+str(i)+'_areawater.zip','r')
    zip_ref.extractall(path+'water/tl_2021_'+str(i)+'_areawater')
    zip_ref.close()
    time.sleep(10)


df=gpd.GeoDataFrame()
for i in quadstatecounty:
    tp=gpd.read_file(path+'water/tl_2021_'+str(i)+'_areawater/tl_2021_'+str(i)+'_areawater.shp')
    tp.crs=4269
    tp=tp.to_crs(4326)
    tp['county']=str(i)
    tp=tp.dissolve(by='county',as_index=False)
    df=df.append(tp,ignore_index=True)
df=df[['county','geometry']].reset_index(drop=True)
df.to_file(filename=path+'water/water20.shp',driver='ESRI Shapefile')



