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
bk=gpd.read_file(path+'shp/quadstatebk.shp')
bk.crs={'init': 'epsg:4326'}
water=gpd.read_file(path+'water/water.shp')
water.crs={'init': 'epsg:4326'}
bkclip=gpd.overlay(bk,water,how='difference')
bkclip.crs={'init': 'epsg:4326'}
bkclip.to_file(filename=path+'shp/quadstatebkclipped.shp',driver='ESRI Shapefile')

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
url='ftp://ftp2.census.gov/geo/tiger/TIGER2017/BG/tl_2017_09_bg.zip'
req=urllib.request.urlopen(url)
file = open(path+'shp/tl_2017_09_bg.zip', "wb")
shutil.copyfileobj(req,file)
file.close()
zip_ref=zipfile.ZipFile(path+'shp/tl_2017_09_bg.zip','r')
zip_ref.extractall(path+'shp/tl_2017_09_bg')
zip_ref.close()
ctbkgp=gpd.read_file(path+'shp/tl_2017_09_bg/tl_2017_09_bg.shp')
time.sleep(10)

# New Jersey
url='ftp://ftp2.census.gov/geo/tiger/TIGER2017/BG/tl_2017_34_bg.zip'
req=urllib.request.urlopen(url)
file = open(path+'shp/tl_2017_34_bg.zip', "wb")
shutil.copyfileobj(req,file)
file.close()
zip_ref=zipfile.ZipFile(path+'shp/tl_2017_34_bg.zip','r')
zip_ref.extractall(path+'shp/tl_2017_34_bg')
zip_ref.close()
njbkgp=gpd.read_file(path+'shp/tl_2017_34_bg/tl_2017_34_bg.shp')
time.sleep(10)

# New York
url='ftp://ftp2.census.gov/geo/tiger/TIGER2017/BG/tl_2017_36_bg.zip'
req=urllib.request.urlopen(url)
file = open(path+'shp/tl_2017_36_bg.zip', "wb")
shutil.copyfileobj(req,file)
file.close()
zip_ref=zipfile.ZipFile(path+'shp/tl_2017_36_bg.zip','r')
zip_ref.extractall(path+'shp/tl_2017_36_bg')
zip_ref.close()
nybkgp=gpd.read_file(path+'shp/tl_2017_36_bg/tl_2017_36_bg.shp')
time.sleep(10)

# Pennsylvania
url='ftp://ftp2.census.gov/geo/tiger/TIGER2017/BG/tl_2017_42_bg.zip'
req=urllib.request.urlopen(url)
file = open(path+'shp/tl_2017_42_bg.zip', "wb")
shutil.copyfileobj(req,file)
file.close()
zip_ref=zipfile.ZipFile(path+'shp/tl_2017_42_bg.zip','r')
zip_ref.extractall(path+'shp/tl_2017_42_bg')
zip_ref.close()
pabkgp=gpd.read_file(path+'shp/tl_2017_42_bg/tl_2017_42_bg.shp')
time.sleep(10)

# Merge Quadstate Census Block Groups
bkgp=gpd.GeoDataFrame()
bkgp=bkgp.append(ctbkgp,ignore_index=True)
bkgp=bkgp.append(njbkgp,ignore_index=True)
bkgp=bkgp.append(nybkgp,ignore_index=True)
bkgp=bkgp.append(pabkgp,ignore_index=True)
bkgp['blockgroupid']=bkgp['GEOID']
bkgp['lat']=pd.to_numeric(bkgp['INTPTLAT'])
bkgp['long']=pd.to_numeric(bkgp['INTPTLON'])
bkgp=bkgp[['blockgroupid','lat','long','geometry']]
bkgp=bkgp.to_crs({'init': 'epsg:4326'})
bkgp.to_file(filename=path+'shp/quadstatebkgp.shp',driver='ESRI Shapefile')

# Convert polygons to centroids
bkgppt=bkgp[['blockgroupid','lat','long']]
bkgppt=gpd.GeoDataFrame(bkgppt,crs={'init': 'epsg:4326'},geometry=[shapely.geometry.Point(xy) for xy in zip(bkgppt.long, bkgppt.lat)])
bkgppt.to_file(filename=path+'shp/quadstatebkgppt.shp',driver='ESRI Shapefile')

# Clip water
bkgp=gpd.read_file(path+'shp/quadstatebkgp.shp')
bkgp.crs={'init': 'epsg:4326'}
water=gpd.read_file(path+'water/water.shp')
water.crs={'init': 'epsg:4326'}
bkgpclip=gpd.overlay(bkgp,water,how='difference')
bkgpclip.crs={'init': 'epsg:4326'}
bkgpclip.to_file(filename=path+'shp/quadstatebkgpclipped.shp',driver='ESRI Shapefile')

# Remove downloaded files
os.remove(path+'shp/tl_2017_09_bg.zip')
shutil.rmtree(path+'shp/tl_2017_09_bg')
os.remove(path+'shp/tl_2017_34_bg.zip')
shutil.rmtree(path+'shp/tl_2017_34_bg')
os.remove(path+'shp/tl_2017_36_bg.zip')
shutil.rmtree(path+'shp/tl_2017_36_bg')
os.remove(path+'shp/tl_2017_42_bg.zip')
shutil.rmtree(path+'shp/tl_2017_42_bg')



# Census Tract
# Download Census Tract shapefiles
# Connecticut
url='ftp://ftp2.census.gov/geo/tiger/TIGER2017/TRACT/tl_2017_09_tract.zip'
req=urllib.request.urlopen(url)
file = open(path+'shp/tl_2017_09_tract.zip', "wb")
shutil.copyfileobj(req,file)
file.close()
zip_ref=zipfile.ZipFile(path+'shp/tl_2017_09_tract.zip','r')
zip_ref.extractall(path+'shp/tl_2017_09_tract')
zip_ref.close()
ctct=gpd.read_file(path+'shp/tl_2017_09_tract/tl_2017_09_tract.shp')
time.sleep(10)

# New Jersey
url='ftp://ftp2.census.gov/geo/tiger/TIGER2017/TRACT/tl_2017_34_tract.zip'
req=urllib.request.urlopen(url)
file = open(path+'shp/tl_2017_34_tract.zip', "wb")
shutil.copyfileobj(req,file)
file.close()
zip_ref=zipfile.ZipFile(path+'shp/tl_2017_34_tract.zip','r')
zip_ref.extractall(path+'shp/tl_2017_34_tract')
zip_ref.close()
njct=gpd.read_file(path+'shp/tl_2017_34_tract/tl_2017_34_tract.shp')
time.sleep(10)

# New York
url='ftp://ftp2.census.gov/geo/tiger/TIGER2017/TRACT/tl_2017_36_tract.zip'
req=urllib.request.urlopen(url)
file = open(path+'shp/tl_2017_36_tract.zip', "wb")
shutil.copyfileobj(req,file)
file.close()
zip_ref=zipfile.ZipFile(path+'shp/tl_2017_36_tract.zip','r')
zip_ref.extractall(path+'shp/tl_2017_36_tract')
zip_ref.close()
nyct=gpd.read_file(path+'shp/tl_2017_36_tract/tl_2017_36_tract.shp')
time.sleep(10)

# Pennsylvania
url='ftp://ftp2.census.gov/geo/tiger/TIGER2017/TRACT/tl_2017_42_tract.zip'
req=urllib.request.urlopen(url)
file = open(path+'shp/tl_2017_42_tract.zip', "wb")
shutil.copyfileobj(req,file)
file.close()
zip_ref=zipfile.ZipFile(path+'shp/tl_2017_42_tract.zip','r')
zip_ref.extractall(path+'shp/tl_2017_42_tract')
zip_ref.close()
pact=gpd.read_file(path+'shp/tl_2017_42_tract/tl_2017_42_tract.shp')
time.sleep(10)

# Merge Quadstate Census Tracts
ct=gpd.GeoDataFrame()
ct=ct.append(ctct,ignore_index=True)
ct=ct.append(njct,ignore_index=True)
ct=ct.append(nyct,ignore_index=True)
ct=ct.append(pact,ignore_index=True)
ct['tractid']=ct['GEOID']
ct['lat']=pd.to_numeric(ct['INTPTLAT'])
ct['long']=pd.to_numeric(ct['INTPTLON'])
ct=ct[['tractid','lat','long','geometry']]
ct=ct.to_crs({'init': 'epsg:4326'})
ct.to_file(filename=path+'shp/quadstatect.shp',driver='ESRI Shapefile')

# Convert polygons to centroids
ctpt=ct[['tractid','lat','long']]
ctpt=gpd.GeoDataFrame(ctpt,crs={'init': 'epsg:4326'},geometry=[shapely.geometry.Point(xy) for xy in zip(ctpt.long, ctpt.lat)])
ctpt.to_file(filename=path+'shp/quadstatectpt.shp',driver='ESRI Shapefile')

# Clip water
ct=gpd.read_file(path+'shp/quadstatect.shp')
ct.crs={'init': 'epsg:4326'}
water=gpd.read_file(path+'water/water.shp')
water.crs={'init': 'epsg:4326'}
ctclip=gpd.overlay(ct,water,how='difference')
ctclip.crs={'init': 'epsg:4326'}
ctclip.to_file(filename=path+'shp/quadstatectclipped.shp',driver='ESRI Shapefile')

# Remove downloaded files
os.remove(path+'shp/tl_2017_09_tract.zip')
shutil.rmtree(path+'shp/tl_2017_09_tract')
os.remove(path+'shp/tl_2017_34_tract.zip')
shutil.rmtree(path+'shp/tl_2017_34_tract')
os.remove(path+'shp/tl_2017_36_tract.zip')
shutil.rmtree(path+'shp/tl_2017_36_tract')
os.remove(path+'shp/tl_2017_42_tract.zip')
shutil.rmtree(path+'shp/tl_2017_42_tract')




