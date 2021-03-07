#! /usr/bin/python3
# Travelshed Mapping
# OpenTripPlanner Guide: http://docs.opentripplanner.org/en/latest/
# OpenTripPlanner API Doc: http://dev.opentripplanner.org/apidoc/1.0.0/index.html

import urllib.request
import shutil
import zipfile
import geopandas as gpd
import pandas as pd
import shapely
import os
import time

pd.set_option('display.max_columns', None)

path='/home/mayijun/TRAVELSHED/'
#path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2018/TRAVELSHEDREVAMP/'
#path='E:/'

url='http://gbfs.citibikenyc.com/gbfs/gbfs.json'
df=pd.read_json(url)
df=pd.DataFrame(df.loc['en','data'])
df.loc[0,'feeds']['url']
url='http://gbfs.citibikenyc.com/gbfs/en/station_status.json'







# Census Block
# Download Census Block shapefiles
# Connecticut
url='ftp://ftp2.census.gov/geo/tiger/TIGER2017/TABBLOCK/tl_2017_09_tabblock10.zip'
req=urllib.request.urlopen(url)
file = open(path+'shp/tl_2017_09_tabblock10.zip', "wb")
shutil.copyfileobj(req,file)
file.close()
zip_ref=zipfile.ZipFile(path+'shp/tl_2017_09_tabblock10.zip','r')
zip_ref.extractall(path+'shp/tl_2017_09_tabblock10')
zip_ref.close()
ctbk=gpd.read_file(path+'shp/tl_2017_09_tabblock10/tl_2017_09_tabblock10.shp')
time.sleep(10)


