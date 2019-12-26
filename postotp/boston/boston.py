#! /usr/bin/python3

import pandas as pd
import geopandas as gpd


path='C:/Users/Y_Ma2/Desktop/BOSTON/'
#path='E:/angela/'
#path='/home/mayijun/TRAVELSHED/'

nyc=['36005','36047','36061','36081','36085']
pd.set_option('display.max_columns', None)

# Summary
df=pd.read_excel(path+'boston.xlsx',sheet_name='Sheet1',dtype=str)
for i in df.columns[1:]:
    df.loc[:,i]=pd.to_numeric(df.loc[:,i])
bk=gpd.read_file(path+'quadstatebk.shp')
boston=bk.merge(df,on='blockid')
boston=boston[['blockid','SITE1','SITE2','SITE3','SITE4','geometry']]
boston.to_file(path+'boston.shp')
