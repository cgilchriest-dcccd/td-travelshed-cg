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
# path='/home/mayijun/TRAVELSHED/'
path='C:/Users/mayij/Desktop/DOC/DCP2018/TRAVELSHEDREVAMP/'
#path='E:/'


# Census Block
# Download Census Block shapefiles
# Connecticut
url='https://www2.census.gov/geo/tiger/TIGER2021/TABBLOCK20/tl_2021_09_tabblock20.zip'
req=urllib.request.urlopen(url)
file=open(path+'shp/tl_2021_09_tabblock20.zip', "wb")
shutil.copyfileobj(req,file)
file.close()
zip_ref=zipfile.ZipFile(path+'shp/tl_2021_09_tabblock20.zip','r')
zip_ref.extractall(path+'shp/tl_2021_09_tabblock20')
zip_ref.close()
time.sleep(10)

# New Jersey
url='https://www2.census.gov/geo/tiger/TIGER2021/TABBLOCK20/tl_2021_34_tabblock20.zip'
req=urllib.request.urlopen(url)
file=open(path+'shp/tl_2021_34_tabblock20.zip', "wb")
shutil.copyfileobj(req,file)
file.close()
zip_ref=zipfile.ZipFile(path+'shp/tl_2021_34_tabblock20.zip','r')
zip_ref.extractall(path+'shp/tl_2021_34_tabblock20')
zip_ref.close()
time.sleep(10)

# New York
url='https://www2.census.gov/geo/tiger/TIGER2021/TABBLOCK20/tl_2021_36_tabblock20.zip'
req=urllib.request.urlopen(url)
file = open(path+'shp/tl_2021_36_tabblock20.zip', "wb")
shutil.copyfileobj(req,file)
file.close()
zip_ref=zipfile.ZipFile(path+'shp/tl_2021_36_tabblock20.zip','r')
zip_ref.extractall(path+'shp/tl_2021_36_tabblock20')
zip_ref.close()
time.sleep(10)

# Pennsylvania
url='https://www2.census.gov/geo/tiger/TIGER2021/TABBLOCK20/tl_2021_42_tabblock20.zip'
req=urllib.request.urlopen(url)
file = open(path+'shp/tl_2021_42_tabblock20.zip', "wb")
shutil.copyfileobj(req,file)
file.close()
zip_ref=zipfile.ZipFile(path+'shp/tl_2021_42_tabblock20.zip','r')
zip_ref.extractall(path+'shp/tl_2021_42_tabblock20')
zip_ref.close()
time.sleep(10)

# Merge Quadstate Census Blocks
ctbk=gpd.read_file(path+'shp/tl_2021_09_tabblock20/tl_2021_09_tabblock20.shp')
ctbk.crs=4269
ctbk=ctbk.to_crs(4326)
njbk=gpd.read_file(path+'shp/tl_2021_34_tabblock20/tl_2021_34_tabblock20.shp')
njbk.crs=4269
njbk=njbk.to_crs(4326)
nybk=gpd.read_file(path+'shp/tl_2021_36_tabblock20/tl_2021_36_tabblock20.shp')
nybk.crs=4269
nybk=nybk.to_crs(4326)
pabk=gpd.read_file(path+'shp/tl_2021_42_tabblock20/tl_2021_42_tabblock20.shp')
pabk.crs=4269
pabk=pabk.to_crs(4326)
bk=pd.concat([ctbk,njbk,nybk,pabk],axis=0,ignore_index=True)
bk['blockid20']=bk['GEOID20']
bk['lat']=pd.to_numeric(bk['INTPTLAT20'])
bk['long']=pd.to_numeric(bk['INTPTLON20'])
bk=bk[['blockid20','lat','long','geometry']].reset_index(drop=True)
bk.to_file(filename=path+'shp/quadstatebk20.shp',driver='ESRI Shapefile')

# Convert polygons to centroids
bk=gpd.read_file(path+'shp/quadstatebk20.shp')
bk.crs=4326
bkpt=bk[['blockid20','lat','long']].reset_index(drop=True)
bkpt=gpd.GeoDataFrame(bkpt,crs=4326,geometry=[shapely.geometry.Point(xy) for xy in zip(bkpt.long, bkpt.lat)])
bkpt.to_file(filename=path+'shp/quadstatebkpt20.shp',driver='ESRI Shapefile')

# Clip water
bk=gpd.read_file(path+'shp/quadstatebk20.shp')
bk.crs=4326
water=gpd.read_file(path+'water/water20.shp')
water.crs=4326
bkclip=gpd.overlay(bk,water,how='difference')
bkclip.to_file(filename=path+'shp/quadstatebkclipped20.shp',driver='ESRI Shapefile')

# Remove downloaded files
os.remove(path+'shp/tl_2017_09_tabblock10.zip')
shutil.rmtree(path+'shp/tl_2017_09_tabblock10')
os.remove(path+'shp/tl_2017_34_tabblock10.zip')
shutil.rmtree(path+'shp/tl_2017_34_tabblock10')
os.remove(path+'shp/tl_2017_36_tabblock10.zip')
shutil.rmtree(path+'shp/tl_2017_36_tabblock10')
os.remove(path+'shp/tl_2017_42_tabblock10.zip')
shutil.rmtree(path+'shp/tl_2017_42_tabblock10')



# Census Block Group
# Download Census Block Group shapefiles
# Connecticut
url='https://www2.census.gov/geo/tiger/TIGER2021/BG/tl_2021_09_bg.zip'
req=urllib.request.urlopen(url)
file = open(path+'shp/tl_2021_09_bg.zip', "wb")
shutil.copyfileobj(req,file)
file.close()
zip_ref=zipfile.ZipFile(path+'shp/tl_2021_09_bg.zip','r')
zip_ref.extractall(path+'shp/tl_2021_09_bg')
zip_ref.close()
time.sleep(10)

# New Jersey
url='https://www2.census.gov/geo/tiger/TIGER2021/BG/tl_2021_34_bg.zip'
req=urllib.request.urlopen(url)
file = open(path+'shp/tl_2021_34_bg.zip', "wb")
shutil.copyfileobj(req,file)
file.close()
zip_ref=zipfile.ZipFile(path+'shp/tl_2021_34_bg.zip','r')
zip_ref.extractall(path+'shp/tl_2021_34_bg')
zip_ref.close()
time.sleep(10)

# New York
url='https://www2.census.gov/geo/tiger/TIGER2021/BG/tl_2021_36_bg.zip'
req=urllib.request.urlopen(url)
file = open(path+'shp/tl_2021_36_bg.zip', "wb")
shutil.copyfileobj(req,file)
file.close()
zip_ref=zipfile.ZipFile(path+'shp/tl_2021_36_bg.zip','r')
zip_ref.extractall(path+'shp/tl_2021_36_bg')
zip_ref.close()
time.sleep(10)

# Pennsylvania
url='https://www2.census.gov/geo/tiger/TIGER2021/BG/tl_2021_42_bg.zip'
req=urllib.request.urlopen(url)
file = open(path+'shp/tl_2021_42_bg.zip', "wb")
shutil.copyfileobj(req,file)
file.close()
zip_ref=zipfile.ZipFile(path+'shp/tl_2021_42_bg.zip','r')
zip_ref.extractall(path+'shp/tl_2021_42_bg')
zip_ref.close()
time.sleep(10)

# Merge Quadstate Census Block Groups
ctbkgp=gpd.read_file(path+'shp/tl_2021_09_bg/tl_2021_09_bg.shp')
ctbkgp.crs=4269
ctbkgp=ctbkgp.to_crs(4326)
njbkgp=gpd.read_file(path+'shp/tl_2021_34_bg/tl_2021_34_bg.shp')
njbkgp.crs=4269
njbkgp=njbkgp.to_crs(4326)
nybkgp=gpd.read_file(path+'shp/tl_2021_36_bg/tl_2021_36_bg.shp')
nybkgp.crs=4269
nybkgp=nybkgp.to_crs(4326)
pabkgp=gpd.read_file(path+'shp/tl_2021_42_bg/tl_2021_42_bg.shp')
pabkgp.crs=4269
pabkgp=pabkgp.to_crs(4326)
bkgp=pd.concat([ctbkgp,njbkgp,nybkgp,pabkgp],axis=0,ignore_index=True)
bkgp['bkgpid20']=bkgp['GEOID']
bkgp['lat']=pd.to_numeric(bkgp['INTPTLAT'])
bkgp['long']=pd.to_numeric(bkgp['INTPTLON'])
bkgp=bkgp[['bkgpid20','lat','long','geometry']].reset_index(drop=True)
bkgp.to_file(filename=path+'shp/quadstatebkgp20.shp',driver='ESRI Shapefile')

# Convert polygons to centroids
bkgp=gpd.read_file(path+'shp/quadstatebkgp20.shp')
bkgp.crs=4326
bkgppt=bkgp[['bkgpid20','lat','long']].reset_index(drop=True)
bkgppt=gpd.GeoDataFrame(bkgppt,crs=4326,geometry=[shapely.geometry.Point(xy) for xy in zip(bkgppt.long, bkgppt.lat)])
bkgppt.to_file(filename=path+'shp/quadstatebkgppt20.shp',driver='ESRI Shapefile')

# Clip water
bkgp=gpd.read_file(path+'shp/quadstatebkgp20.shp')
bkgp.crs=4326
water=gpd.read_file(path+'water/water20.shp')
water.crs=4326
bkgpclip=gpd.overlay(bkgp,water,how='difference')
bkgpclip.to_file(filename=path+'shp/quadstatebkgpclipped20.shp',driver='ESRI Shapefile')

# Remove downloaded files
os.remove(path+'shp/tl_2021_09_bg.zip')
shutil.rmtree(path+'shp/tl_2021_09_bg')
os.remove(path+'shp/tl_2021_34_bg.zip')
shutil.rmtree(path+'shp/tl_2021_34_bg')
os.remove(path+'shp/tl_2021_36_bg.zip')
shutil.rmtree(path+'shp/tl_2021_36_bg')
os.remove(path+'shp/tl_2021_42_bg.zip')
shutil.rmtree(path+'shp/tl_2021_42_bg')



# Census Tract
# Download Census Tract shapefiles
# Connecticut
url='https://www2.census.gov/geo/tiger/TIGER2021/TRACT/tl_2021_09_tract.zip'
req=urllib.request.urlopen(url)
file = open(path+'shp/tl_2021_09_tract.zip', "wb")
shutil.copyfileobj(req,file)
file.close()
zip_ref=zipfile.ZipFile(path+'shp/tl_2021_09_tract.zip','r')
zip_ref.extractall(path+'shp/tl_2021_09_tract')
zip_ref.close()
time.sleep(10)

# New Jersey
url='https://www2.census.gov/geo/tiger/TIGER2021/TRACT/tl_2021_34_tract.zip'
req=urllib.request.urlopen(url)
file=open(path+'shp/tl_2021_34_tract.zip', "wb")
shutil.copyfileobj(req,file)
file.close()
zip_ref=zipfile.ZipFile(path+'shp/tl_2021_34_tract.zip','r')
zip_ref.extractall(path+'shp/tl_2021_34_tract')
zip_ref.close()
time.sleep(10)

# New York
url='https://www2.census.gov/geo/tiger/TIGER2021/TRACT/tl_2021_36_tract.zip'
req=urllib.request.urlopen(url)
file=open(path+'shp/tl_2021_36_tract.zip', "wb")
shutil.copyfileobj(req,file)
file.close()
zip_ref=zipfile.ZipFile(path+'shp/tl_2021_36_tract.zip','r')
zip_ref.extractall(path+'shp/tl_2021_36_tract')
zip_ref.close()
time.sleep(10)

# Pennsylvania
url='https://www2.census.gov/geo/tiger/TIGER2021/TRACT/tl_2021_42_tract.zip'
req=urllib.request.urlopen(url)
file=open(path+'shp/tl_2021_42_tract.zip', "wb")
shutil.copyfileobj(req,file)
file.close()
zip_ref=zipfile.ZipFile(path+'shp/tl_2021_42_tract.zip','r')
zip_ref.extractall(path+'shp/tl_2021_42_tract')
zip_ref.close()
time.sleep(10)

# Merge Quadstate Census Tracts
ctct=gpd.read_file(path+'shp/tl_2021_09_tract/tl_2021_09_tract.shp')
ctct.crs=4269
ctct=ctct.to_crs(4326)
njct=gpd.read_file(path+'shp/tl_2021_34_tract/tl_2021_34_tract.shp')
njct.crs=4269
njct=njct.to_crs(4326)
nyct=gpd.read_file(path+'shp/tl_2021_36_tract/tl_2021_36_tract.shp')
nyct.crs=4269
nyct=nyct.to_crs(4326)
pact=gpd.read_file(path+'shp/tl_2021_42_tract/tl_2021_42_tract.shp')
pact.crs=4269
pact=pact.to_crs(4326)
ct=pd.concat([ctct,njct,nyct,pact],axis=0,ignore_index=True)
ct['tractid20']=ct['GEOID']
ct['lat']=pd.to_numeric(ct['INTPTLAT'])
ct['long']=pd.to_numeric(ct['INTPTLON'])
ct=ct[['tractid20','lat','long','geometry']].reset_index(drop=True)
ct.to_file(filename=path+'shp/quadstatect20.shp',driver='ESRI Shapefile')

# Convert polygons to centroids
ct=gpd.read_file(path+'shp/quadstatect20.shp')
ct.crs=4326
ctpt=ct[['tractid20','lat','long']].reset_index(drop=True)
ctpt=gpd.GeoDataFrame(ctpt,crs=4326,geometry=[shapely.geometry.Point(xy) for xy in zip(ctpt.long, ctpt.lat)])
ctpt.to_file(filename=path+'shp/quadstatectpt20.shp',driver='ESRI Shapefile')

# Clip water
ct=gpd.read_file(path+'shp/quadstatect20.shp')
ct.crs=4326
water=gpd.read_file(path+'water/water20.shp')
water.crs=4326
ctclip=gpd.overlay(ct,water,how='difference')
ctclip.to_file(filename=path+'shp/quadstatectclipped20.shp',driver='ESRI Shapefile')

# Remove downloaded files
os.remove(path+'shp/tl_2017_09_tract.zip')
shutil.rmtree(path+'shp/tl_2017_09_tract')
os.remove(path+'shp/tl_2017_34_tract.zip')
shutil.rmtree(path+'shp/tl_2017_34_tract')
os.remove(path+'shp/tl_2017_36_tract.zip')
shutil.rmtree(path+'shp/tl_2017_36_tract')
os.remove(path+'shp/tl_2017_42_tract.zip')
shutil.rmtree(path+'shp/tl_2017_42_tract')




