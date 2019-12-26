#! /usr/bin/python3
# Travelshed Mapping
# OpenTripPlanner Guide: http://docs.opentripplanner.org/en/latest/
# OpenTripPlanner API Doc: http://dev.opentripplanner.org/apidoc/1.0.0/index.html

import geopandas as gpd
import pandas as pd
import os

path='/home/mayijun/TRAVELSHED/'
#path='E:/TRAVELSHED/'


# inbound
#location=pd.read_excel(path+'regionalrailnew/inbound/input2.xlsx',sheet_name='input',dtype=str)
#oldsiteid=[str(x)+'pr.csv' for x in location.oldsiteid]
#
#for filename in os.listdir(path+'regionalrailnew/inbound/'):
#    if filename in oldsiteid:
#        os.rename(path+'regionalrailnew/inbound/'+filename, path+'regionalrailnew/inbound/'+list(location.loc[location.oldsiteid==filename.replace('pr.csv',''),'newsiteid'])[0]+'pr.csv')

location=pd.read_excel(path+'regionalrailnew/inbound/input2.xlsx',sheet_name='input',dtype=str)
newsiteid=[str(x)+'pr.csv' for x in location.newsiteid]

for filename in os.listdir(path+'regionalrailnew/inbound/'):
    if filename in newsiteid:
        df=pd.read_csv(path+'regionalrailnew/inbound/'+filename,dtype=str)
        df.columns=['blockid',filename.replace('pr.csv','')]
        df.to_csv(path+'regionalrailnew/inbound/'+filename,index=False)



# outbound
#location=pd.read_excel(path+'regionalrailnew/outbound/input2.xlsx',sheet_name='input',dtype=str)
#oldsiteid=[str(x)+'pr.csv' for x in location.oldsiteid]
#
#for filename in os.listdir(path+'regionalrailnew/outbound/'):
#    if filename in oldsiteid:
#        os.rename(path+'regionalrailnew/outbound/'+filename, path+'regionalrailnew/outbound/'+list(location.loc[location.oldsiteid==filename.replace('pr.csv',''),'newsiteid'])[0]+'pr.csv')


location=pd.read_excel(path+'regionalrailnew/outbound/input2.xlsx',sheet_name='input',dtype=str)
newsiteid=[str(x)+'pr.csv' for x in location.newsiteid]

for filename in os.listdir(path+'regionalrailnew/outbound/'):
    if filename in newsiteid:
        df=pd.read_csv(path+'regionalrailnew/outbound/'+filename,dtype=str)
        df.columns=['blockid',filename.replace('pr.csv','')]
        df.to_csv(path+'regionalrailnew/outbound/'+filename,index=False)