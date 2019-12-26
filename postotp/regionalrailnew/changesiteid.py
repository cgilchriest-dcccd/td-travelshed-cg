#! /usr/bin/python3
# Travelshed Mapping
# OpenTripPlanner Guide: http://docs.opentripplanner.org/en/latest/
# OpenTripPlanner API Doc: http://dev.opentripplanner.org/apidoc/1.0.0/index.html

import pandas as pd

#path='/home/mayijun/TRAVELSHED/'
#path='E:/TRAVELSHED/'
path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2018/TRAVELSHEDREVAMP/'


# inbound
location=pd.read_excel(path+'regionalrailnew/inbound/input.xlsx',sheet_name='input',dtype=str)
for i in location['siteid']:
    df=pd.read_csv(path+'regionalrailnew/inbound/'+i+'wt.csv',dtype=str)
    df.columns=['blockid',list(location.loc[location['siteid']==i,'newsiteid'])[0]]
    df.to_csv(path+'regionalrailnew/inbound/'+list(location.loc[location['siteid']==i,'newsiteid'])[0]+'wt.csv',index=False)
    df=pd.read_csv(path+'regionalrailnew/inbound/'+i+'pr.csv',dtype=str)
    df.columns=['blockid',list(location.loc[location['siteid']==i,'newsiteid'])[0]]
    df.to_csv(path+'regionalrailnew/inbound/'+list(location.loc[location['siteid']==i,'newsiteid'])[0]+'pr.csv',index=False)


# outbound
location=pd.read_excel(path+'regionalrailnew/outbound/input.xlsx',sheet_name='input',dtype=str)
for i in location['siteid']:
    df=pd.read_csv(path+'regionalrailnew/outbound/'+i+'wt.csv',dtype=str)
    df.columns=['blockid',list(location.loc[location['siteid']==i,'newsiteid'])[0]]
    df.to_csv(path+'regionalrailnew/outbound/'+list(location.loc[location['siteid']==i,'newsiteid'])[0]+'wt.csv',index=False)
    df=pd.read_csv(path+'regionalrailnew/outbound/'+i+'pr.csv',dtype=str)
    df.columns=['blockid',list(location.loc[location['siteid']==i,'newsiteid'])[0]]
    df.to_csv(path+'regionalrailnew/outbound/'+list(location.loc[location['siteid']==i,'newsiteid'])[0]+'pr.csv',index=False)