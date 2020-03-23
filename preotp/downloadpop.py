#! /usr/bin/python3
# Travelshed Mapping
# OpenTripPlanner Guide: http://docs.opentripplanner.org/en/latest/
# OpenTripPlanner API Doc: http://dev.opentripplanner.org/apidoc/1.0.0/index.html

import requests
import pandas as pd

pd.set_option('display.max_columns', None)
#path='/home/mayijun/TRAVELSHED/'
path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2018/TRAVELSHEDREVAMP/'
#path='E:/'

apikey=pd.read_csv(path+'population/apikey.txt',header=None).loc[0,0]


# Download ACS2014-2018 Population Data through Census Bureau API
rs=requests.get('https://api.census.gov/data/2018/acs/acs5?get=NAME,group(B01001)&for=tract:*&in=state:09&key='+apikey).json()
rs=pd.DataFrame(rs)
rs.columns=rs.loc[0]
rs=rs.loc[1:]
ct=rs[['GEO_ID','B01001_001E']].reset_index(drop=True)
ct['tractid']=[str(x)[9:20] for x in ct['GEO_ID']]
ct['pop']=pd.to_numeric(ct['B01001_001E'])
ct=ct[['tractid','pop']].reset_index(drop=True)

rs=requests.get('https://api.census.gov/data/2018/acs/acs5?get=NAME,group(B01001)&for=tract:*&in=state:34&key='+apikey).json()
rs=pd.DataFrame(rs)
rs.columns=rs.loc[0]
rs=rs.loc[1:]
nj=rs[['GEO_ID','B01001_001E']].reset_index(drop=True)
nj['tractid']=[str(x)[9:20] for x in nj['GEO_ID']]
nj['pop']=pd.to_numeric(nj['B01001_001E'])
nj=nj[['tractid','pop']].reset_index(drop=True)

rs=requests.get('https://api.census.gov/data/2018/acs/acs5?get=NAME,group(B01001)&for=tract:*&in=state:36&key='+apikey).json()
rs=pd.DataFrame(rs)
rs.columns=rs.loc[0]
rs=rs.loc[1:]
ny=rs[['GEO_ID','B01001_001E']].reset_index(drop=True)
ny['tractid']=[str(x)[9:20] for x in ny['GEO_ID']]
ny['pop']=pd.to_numeric(ny['B01001_001E'])
ny=ny[['tractid','pop']].reset_index(drop=True)

rs=requests.get('https://api.census.gov/data/2018/acs/acs5?get=NAME,group(B01001)&for=tract:*&in=state:42&key='+apikey).json()
rs=pd.DataFrame(rs)
rs.columns=rs.loc[0]
rs=rs.loc[1:]
pa=rs[['GEO_ID','B01001_001E']].reset_index(drop=True)
pa['tractid']=[str(x)[9:20] for x in pa['GEO_ID']]
pa['pop']=pd.to_numeric(pa['B01001_001E'])
pa=pa[['tractid','pop']].reset_index(drop=True)

pop=pd.concat([ct,nj,ny,pa],axis=0,ignore_index=True)
pop.to_csv(path+'population/tractpop2018.csv',index=False)
