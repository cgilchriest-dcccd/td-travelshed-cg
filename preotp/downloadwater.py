#! /usr/bin/python3
# Travelshed Mapping
# OpenTripPlanner Guide: http://docs.opentripplanner.org/en/latest/
# OpenTripPlanner API Doc: http://dev.opentripplanner.org/apidoc/1.0.0/index.html


import zipfile
import geopandas as gpd
import pandas as pd


pd.set_option('display.max_columns', None)

#path='/home/mayijun/TRAVELSHED/'
path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2018/TRAVELSHEDREVAMP/'


quadstatecounty=['09'+str(x).zfill(3) for x in range(1,17,2)]
quadstatecounty.extend(['34'+str(x).zfill(3) for x in range(1,43,2)])
quadstatecounty.extend(['36'+str(x).zfill(3) for x in range(1,125,2)])
quadstatecounty.extend(['42'+str(x).zfill(3) for x in range(1,135,2)])


df=gpd.GeoDataFrame()
for i in quadstatecounty:
    zip_ref=zipfile.ZipFile(path+'water/download/tl_2018_'+str(i)+'_areawater.zip','r')
    zip_ref.extractall(path+'water/tl_2018_'+str(i)+'_areawater')
    zip_ref.close()
    tp=gpd.read_file(path+'water/tl_2018_'+str(i)+'_areawater/tl_2018_'+str(i)+'_areawater.shp') # crs 4326
    tp['county']=str(i)
    tp=tp.dissolve(by='county',as_index=False)
    df=df.append(tp,ignore_index=True)
df=df[['county','geometry']]
df=df.to_crs({'init': 'epsg:4326'})
df.to_file(filename=path+'water/water.shp',driver='ESRI Shapefile')
