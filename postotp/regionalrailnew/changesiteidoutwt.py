#! /usr/bin/python3
# Travelshed Mapping
# OpenTripPlanner Guide: http://docs.opentripplanner.org/en/latest/
# OpenTripPlanner API Doc: http://dev.opentripplanner.org/apidoc/1.0.0/index.html

import geopandas as gpd
import pandas as pd
import os

path='/home/mayijun/TRAVELSHED/'
#path='E:/TRAVELSHED/'




# outbound
location=pd.read_excel(path+'regionalrailnew/outbound/input2.xlsx',sheet_name='input',dtype=str)
oldsiteid=[str(x)+'wt.csv' for x in location.oldsiteid]

#for filename in os.listdir(path+'regionalrailnew/outbound/'):
#    if filename in oldsiteid:
#        os.rename(path+'regionalrailnew/outbound/'+filename, path+'regionalrailnew/outbound/'+list(location.loc[location.oldsiteid==filename.replace('wt.csv',''),'newsiteid'])[0]+'wt.csv')
#
wtbk=gpd.read_file(path+'regionalrailnew/outbound/wtbk.shp')
wtbk.crs={'init': 'epsg:4326'}
wtbk.columns=['blockid']+list(location.newsiteid)+['geometry']
wtbk.to_file(path+'regionalrailnew/outbound/wtbk.shp')


#location=pd.read_excel(path+'regionalrailnew/outbound/input2.xlsx',sheet_name='input',dtype=str)
#newsiteid=[str(x)+'wt.csv' for x in location.newsiteid]
#
#for filename in os.listdir(path+'regionalrailnew/outbound/'):
#    if filename in newsiteid:
#        df=pd.read_csv(path+'regionalrailnew/outbound/'+filename,dtype=str)
#        df.columns=['blockid',filename.replace('wt.csv','')]
#        df.to_csv(path+'regionalrailnew/outbound/'+filename,index=False)