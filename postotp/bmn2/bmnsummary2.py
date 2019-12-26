#! /usr/bin/python3
# Travelshed Mapping
# OpenTripPlanner Guide: http://docs.opentripplanner.org/en/latest/
# OpenTripPlanner API Doc: http://dev.opentripplanner.org/apidoc/1.0.0/index.html

import matplotlib
matplotlib.use('Agg')

import datetime
import time
import geopandas as gpd
import pandas as pd
import numpy as np
import shapely
import requests
import multiprocessing as mp
import matplotlib.pyplot as plt
import contextily as ctx
import mpl_toolkits.axes_grid1

start=datetime.datetime.now()

pd.set_option('display.max_columns', None)
path='/home/mayijun/TRAVELSHED/'
#path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2018/TRAVELSHEDREVAMP/'
#path='C:/Users/Y_Ma2/Desktop/TEST/'
#path='C:/Users/Y_Ma2/Desktop/amazon/'
#path='E:/TRAVELSHEDREVAMP/'
#doserver='http://142.93.21.138:8801/'
doserver='http://localhost:8801/'



## Block level summary
#bmnbk=gpd.read_file(path+'shp/quadstatebkclipped.shp')
#bmnbk.crs={'init': 'epsg:4326'}
#bmnbk=bmnbk[['blockid','geometry']]    
#bmnbk=bmnbk.set_index('blockid')
#
#exwtbk=pd.read_csv(path+'bmn2/exwtbk.csv',dtype=float,converters={'blockid':str})
#exwtbk=exwtbk.set_index('blockid')
#exwtbk.columns=[str(x).replace('BMN','')+'EXWT' for x in exwtbk.columns]
#bmnbk=pd.concat([bmnbk,exwtbk],axis=1)
#
#exprbk=pd.read_csv(path+'bmn2/exprbk.csv',dtype=float,converters={'blockid':str})
#exprbk=exprbk.set_index('blockid')
#exprbk.columns=[str(x).replace('BMN','')+'EXPR' for x in exprbk.columns]
#bmnbk=pd.concat([bmnbk,exprbk],axis=1)
#
#exwtprbk=pd.read_csv(path+'bmn2/exwtprbk.csv',dtype=float,converters={'blockid':str})
#exwtprbk=exwtprbk.set_index('blockid')
#exwtprbk.columns=[str(x).replace('BMN','')+'EXWTPR' for x in exwtprbk.columns]
#bmnbk=pd.concat([bmnbk,exwtprbk],axis=1)
#
#psaesawtbk=pd.read_csv(path+'bmn2/psaesawtbk.csv',dtype=float,converters={'blockid':str})
#psaesawtbk=psaesawtbk.set_index('blockid')
#psaesawtbk.columns=[str(x).replace('BMN','')+'PSAWT' for x in psaesawtbk.columns]
#bmnbk=pd.concat([bmnbk,psaesawtbk],axis=1)
#
#psaesaprbk=pd.read_csv(path+'bmn2/psaesaprbk.csv',dtype=float,converters={'blockid':str})
#psaesaprbk=psaesaprbk.set_index('blockid')
#psaesaprbk.columns=[str(x).replace('BMN','')+'PSAPR' for x in psaesaprbk.columns]
#bmnbk=pd.concat([bmnbk,psaesaprbk],axis=1)
#
#psaesawtprbk=pd.read_csv(path+'bmn2/psaesawtprbk.csv',dtype=float,converters={'blockid':str})
#psaesawtprbk=psaesawtprbk.set_index('blockid')
#psaesawtprbk.columns=[str(x).replace('BMN','')+'PSAWTPR' for x in psaesawtprbk.columns]
#bmnbk=pd.concat([bmnbk,psaesawtprbk],axis=1)
#
#ssywtbk=pd.read_csv(path+'bmn2/ssywtbk.csv',dtype=float,converters={'blockid':str})
#ssywtbk=ssywtbk.set_index('blockid')
#ssywtbk.columns=[str(x).replace('BMN','')+'SSYWT' for x in ssywtbk.columns]
#bmnbk=pd.concat([bmnbk,ssywtbk],axis=1)
#
#ssyprbk=pd.read_csv(path+'bmn2/ssyprbk.csv',dtype=float,converters={'blockid':str})
#ssyprbk=ssyprbk.set_index('blockid')
#ssyprbk.columns=[str(x).replace('BMN','')+'SSYPR' for x in ssyprbk.columns]
#bmnbk=pd.concat([bmnbk,ssyprbk],axis=1)
#
#ssywtprbk=pd.read_csv(path+'bmn2/ssywtprbk.csv',dtype=float,converters={'blockid':str})
#ssywtprbk=ssywtprbk.set_index('blockid')
#ssywtprbk.columns=[str(x).replace('BMN','')+'SSYWTPR' for x in ssywtprbk.columns]
#bmnbk=pd.concat([bmnbk,ssywtprbk],axis=1)
#
#bmnbk=bmnbk.reset_index()
#bmnbk.columns=['blockid']+list(bmnbk.columns[1:])
#loclist=[str(x).replace('EXWT','') for x in bmnbk.columns[2:10]]
#for i in loclist:
#    bmnbk[i+'PSAWTD']=np.where(bmnbk[i+'EXWT']==999,999,np.where(bmnbk[i+'PSAWT']==999,999,bmnbk[i+'EXWT']-bmnbk[i+'PSAWT']))
#for i in loclist:
#    bmnbk[i+'PSAWPD']=np.where(bmnbk[i+'EXWTPR']==999,999,np.where(bmnbk[i+'PSAWTPR']==999,999,bmnbk[i+'EXWTPR']-bmnbk[i+'PSAWTPR']))
#for i in loclist:
#    bmnbk[i+'SSYWTD']=np.where(bmnbk[i+'EXWT']==999,999,np.where(bmnbk[i+'SSYWT']==999,999,bmnbk[i+'EXWT']-bmnbk[i+'SSYWT']))
#for i in loclist:
#    bmnbk[i+'SSYWPD']=np.where(bmnbk[i+'EXWTPR']==999,999,np.where(bmnbk[i+'SSYWTPR']==999,999,bmnbk[i+'EXWTPR']-bmnbk[i+'SSYWTPR']))
#bmnbk.to_file(path+'bmn2/bmnbk2.shp')
#bmnbk=bmnbk.drop('geometry',axis=1)
#bmnbk.to_csv(path+'bmn2/bmnbk2.csv',index=False)
#
#
#
## Tract level summary
#bmnct=gpd.read_file(path+'shp/quadstatectclipped.shp')
#bmnct.crs={'init': 'epsg:4326'}
#bmnct=bmnct[['tractid','geometry']]    
#bmnct=bmnct.set_index('tractid')
#
#exwtct=pd.read_csv(path+'bmn2/exwtct.csv',dtype=float,converters={'tractid':str})
#exwtct=exwtct.set_index('tractid')
#exwtct.columns=[str(x).replace('BMN','')+'EXWT' for x in exwtct.columns]
#bmnct=pd.concat([bmnct,exwtct],axis=1)
#
#exprct=pd.read_csv(path+'bmn2/exprct.csv',dtype=float,converters={'tractid':str})
#exprct=exprct.set_index('tractid')
#exprct.columns=[str(x).replace('BMN','')+'EXPR' for x in exprct.columns]
#bmnct=pd.concat([bmnct,exprct],axis=1)
#
#exwtprct=pd.read_csv(path+'bmn2/exwtprct.csv',dtype=float,converters={'tractid':str})
#exwtprct=exwtprct.set_index('tractid')
#exwtprct.columns=[str(x).replace('BMN','')+'EXWTPR' for x in exwtprct.columns]
#bmnct=pd.concat([bmnct,exwtprct],axis=1)
#
#psaesawtct=pd.read_csv(path+'bmn2/psaesawtct.csv',dtype=float,converters={'tractid':str})
#psaesawtct=psaesawtct.set_index('tractid')
#psaesawtct.columns=[str(x).replace('BMN','')+'PSAWT' for x in psaesawtct.columns]
#bmnct=pd.concat([bmnct,psaesawtct],axis=1)
#
#psaesaprct=pd.read_csv(path+'bmn2/psaesaprct.csv',dtype=float,converters={'tractid':str})
#psaesaprct=psaesaprct.set_index('tractid')
#psaesaprct.columns=[str(x).replace('BMN','')+'PSAPR' for x in psaesaprct.columns]
#bmnct=pd.concat([bmnct,psaesaprct],axis=1)
#
#psaesawtprct=pd.read_csv(path+'bmn2/psaesawtprct.csv',dtype=float,converters={'tractid':str})
#psaesawtprct=psaesawtprct.set_index('tractid')
#psaesawtprct.columns=[str(x).replace('BMN','')+'PSAWTPR' for x in psaesawtprct.columns]
#bmnct=pd.concat([bmnct,psaesawtprct],axis=1)
#
#ssywtct=pd.read_csv(path+'bmn2/ssywtct.csv',dtype=float,converters={'tractid':str})
#ssywtct=ssywtct.set_index('tractid')
#ssywtct.columns=[str(x).replace('BMN','')+'SSYWT' for x in ssywtct.columns]
#bmnct=pd.concat([bmnct,ssywtct],axis=1)
#
#ssyprct=pd.read_csv(path+'bmn2/ssyprct.csv',dtype=float,converters={'tractid':str})
#ssyprct=ssyprct.set_index('tractid')
#ssyprct.columns=[str(x).replace('BMN','')+'SSYPR' for x in ssyprct.columns]
#bmnct=pd.concat([bmnct,ssyprct],axis=1)
#
#ssywtprct=pd.read_csv(path+'bmn2/ssywtprct.csv',dtype=float,converters={'tractid':str})
#ssywtprct=ssywtprct.set_index('tractid')
#ssywtprct.columns=[str(x).replace('BMN','')+'SSYWTPR' for x in ssywtprct.columns]
#bmnct=pd.concat([bmnct,ssywtprct],axis=1)
#
#bmnct=bmnct.reset_index()
#bmnct.columns=['tractid']+list(bmnct.columns[1:])
#loclist=[str(x).replace('EXWT','') for x in bmnct.columns[2:10]]
#for i in loclist:
#    bmnct[i+'PSAWTD']=np.where(bmnct[i+'EXWT']==999,999,np.where(bmnct[i+'PSAWT']==999,999,bmnct[i+'EXWT']-bmnct[i+'PSAWT']))
#for i in loclist:
#    bmnct[i+'PSAWPD']=np.where(bmnct[i+'EXWTPR']==999,999,np.where(bmnct[i+'PSAWTPR']==999,999,bmnct[i+'EXWTPR']-bmnct[i+'PSAWTPR']))
#for i in loclist:
#    bmnct[i+'SSYWTD']=np.where(bmnct[i+'EXWT']==999,999,np.where(bmnct[i+'SSYWT']==999,999,bmnct[i+'EXWT']-bmnct[i+'SSYWT']))
#for i in loclist:
#    bmnct[i+'SSYWPD']=np.where(bmnct[i+'EXWTPR']==999,999,np.where(bmnct[i+'SSYWTPR']==999,999,bmnct[i+'EXWTPR']-bmnct[i+'SSYWTPR']))
#bmnct.to_file(path+'bmn2/bmnct2.shp')
#bmnct=bmnct.drop('geometry',axis=1)
#bmnct.to_csv(path+'bmn2/bmnct2.csv',index=False)



# Join LEHD
bkts=pd.read_csv(path+'bmn2/bmnbk2.csv',dtype=float,converters={'blockid':str})

privaterac=pd.DataFrame()
for i in ['ct','nj','ny','pa']:
    privaterac=pd.concat([privaterac,pd.read_csv(path+'lehd/'+str(i)+'_rac_S000_JT03_2017.csv',dtype=str)[['h_geocode','C000']].reset_index(drop=True)],axis=0,ignore_index=True)
privaterac.columns=['blockid','privaterac']

privatewac=pd.DataFrame()
for i in ['ct','nj','ny','pa']:
    privatewac=pd.concat([privatewac,pd.read_csv(path+'lehd/'+str(i)+'_wac_S000_JT03_2017.csv',dtype=str)[['w_geocode','C000']].reset_index(drop=True)],axis=0,ignore_index=True)
privatewac.columns=['blockid','privatewac']

totalrac=pd.DataFrame()
for i in ['ct','nj','ny','pa']:
    totalrac=pd.concat([totalrac,pd.read_csv(path+'lehd/'+str(i)+'_rac_S000_JT01_2017.csv',dtype=str)[['h_geocode','C000']].reset_index(drop=True)],axis=0,ignore_index=True)
totalrac.columns=['blockid','totalrac']

totalwac=pd.DataFrame()
for i in ['ct','nj','ny','pa']:
    totalwac=pd.concat([totalwac,pd.read_csv(path+'lehd/'+str(i)+'_wac_S000_JT01_2017.csv',dtype=str)[['w_geocode','C000']].reset_index(drop=True)],axis=0,ignore_index=True)
totalwac.columns=['blockid','totalwac']

publicrac=pd.merge(totalrac,privaterac,how='left',on='blockid')
publicrac['privaterac']=publicrac['privaterac'].fillna('0')
publicrac['totalrac']=pd.to_numeric(publicrac['totalrac'])
publicrac['privaterac']=pd.to_numeric(publicrac['privaterac'])
publicrac['publicrac']=publicrac['totalrac']-publicrac['privaterac']

publicwac=pd.merge(totalwac,privatewac,how='left',on='blockid')
publicwac['privatewac']=publicwac['privatewac'].fillna('0')
publicwac['totalwac']=pd.to_numeric(publicwac['totalwac'])
publicwac['privatewac']=pd.to_numeric(publicwac['privatewac'])
publicwac['publicwac']=publicwac['totalwac']-publicwac['privatewac']

df=pd.merge(bkts,publicrac,how='left',on='blockid')
df=pd.merge(df,publicwac,how='left',on='blockid')
df['totalrac']=df['totalrac'].fillna(0)
df['privaterac']=df['privaterac'].fillna(0)
df['publicrac']=df['publicrac'].fillna(0)
df['totalwac']=df['totalwac'].fillna(0)
df['privatewac']=df['privatewac'].fillna(0)
df['publicwac']=df['publicwac'].fillna(0)

loclist=df.columns[1:-38]
for i in loclist:
    df[i]=np.where(df[i]<=15,7.5,
          np.where(df[i]<=30,22.5,
          np.where(df[i]<=45,37.5,
          np.where(df[i]<=60,52.5,
          np.where(df[i]<=75,67.5,
          np.where(df[i]<=90,82.5,
          np.where(df[i]<=105,97.5,
          np.where(df[i]<=120,112.5,
          np.nan))))))))
bmn=pd.DataFrame(index=loclist,columns=['TTLF1-15','TTLF16-30','TTLF31-45','TTLF46-60','TTLF61-75','TTLF76-90','TTLF91-105','TTLF106-120',
                                        'PRLF1-15','PRLF16-30','PRLF31-45','PRLF46-60','PRLF61-75','PRLF76-90','PRLF91-105','PRLF106-120',
                                        'PULF1-15','PULF16-30','PULF31-45','PULF46-60','PULF61-75','PULF76-90','PULF91-105','PULF106-120',
                                        'TTJB1-15','TTJB16-30','TTJB31-45','TTJB46-60','TTJB61-75','TTJB76-90','TTJB91-105','TTJB106-120',
                                        'PRJB1-15','PRJB16-30','PRJB31-45','PRJB46-60','PRJB61-75','PRJB76-90','PRJB91-105','PRJB106-120',
                                        'PUJB1-15','PUJB16-30','PUJB31-45','PUJB46-60','PUJB61-75','PUJB76-90','PUJB91-105','PUJB106-120'])
for i in loclist:
    bmn.loc[i,'TTLF1-15']=sum(df.loc[df[i]==7.5,'totalrac'])
    bmn.loc[i,'TTLF16-30']=sum(df.loc[df[i]==22.5,'totalrac'])
    bmn.loc[i,'TTLF31-45']=sum(df.loc[df[i]==37.5,'totalrac'])
    bmn.loc[i,'TTLF46-60']=sum(df.loc[df[i]==52.5,'totalrac'])
    bmn.loc[i,'TTLF61-75']=sum(df.loc[df[i]==67.5,'totalrac'])
    bmn.loc[i,'TTLF76-90']=sum(df.loc[df[i]==82.5,'totalrac'])
    bmn.loc[i,'TTLF91-105']=sum(df.loc[df[i]==97.5,'totalrac'])
    bmn.loc[i,'TTLF106-120']=sum(df.loc[df[i]==112.5,'totalrac'])
    bmn.loc[i,'PRLF1-15']=sum(df.loc[df[i]==7.5,'privaterac'])
    bmn.loc[i,'PRLF16-30']=sum(df.loc[df[i]==22.5,'privaterac'])
    bmn.loc[i,'PRLF31-45']=sum(df.loc[df[i]==37.5,'privaterac'])
    bmn.loc[i,'PRLF46-60']=sum(df.loc[df[i]==52.5,'privaterac'])
    bmn.loc[i,'PRLF61-75']=sum(df.loc[df[i]==67.5,'privaterac'])
    bmn.loc[i,'PRLF76-90']=sum(df.loc[df[i]==82.5,'privaterac'])
    bmn.loc[i,'PRLF91-105']=sum(df.loc[df[i]==97.5,'privaterac'])
    bmn.loc[i,'PRLF106-120']=sum(df.loc[df[i]==112.5,'privaterac'])
    bmn.loc[i,'PULF1-15']=sum(df.loc[df[i]==7.5,'publicrac'])
    bmn.loc[i,'PULF16-30']=sum(df.loc[df[i]==22.5,'publicrac'])
    bmn.loc[i,'PULF31-45']=sum(df.loc[df[i]==37.5,'publicrac'])
    bmn.loc[i,'PULF46-60']=sum(df.loc[df[i]==52.5,'publicrac'])
    bmn.loc[i,'PULF61-75']=sum(df.loc[df[i]==67.5,'publicrac'])
    bmn.loc[i,'PULF76-90']=sum(df.loc[df[i]==82.5,'publicrac'])
    bmn.loc[i,'PULF91-105']=sum(df.loc[df[i]==97.5,'publicrac'])
    bmn.loc[i,'PULF106-120']=sum(df.loc[df[i]==112.5,'publicrac']) 
    bmn.loc[i,'TTJB1-15']=sum(df.loc[df[i]==7.5,'totalwac'])
    bmn.loc[i,'TTJB16-30']=sum(df.loc[df[i]==22.5,'totalwac'])
    bmn.loc[i,'TTJB31-45']=sum(df.loc[df[i]==37.5,'totalwac'])
    bmn.loc[i,'TTJB46-60']=sum(df.loc[df[i]==52.5,'totalwac'])
    bmn.loc[i,'TTJB61-75']=sum(df.loc[df[i]==67.5,'totalwac'])
    bmn.loc[i,'TTJB76-90']=sum(df.loc[df[i]==82.5,'totalwac'])
    bmn.loc[i,'TTJB91-105']=sum(df.loc[df[i]==97.5,'totalwac'])
    bmn.loc[i,'TTJB106-120']=sum(df.loc[df[i]==112.5,'totalwac'])
    bmn.loc[i,'PRJB1-15']=sum(df.loc[df[i]==7.5,'privatewac'])
    bmn.loc[i,'PRJB16-30']=sum(df.loc[df[i]==22.5,'privatewac'])
    bmn.loc[i,'PRJB31-45']=sum(df.loc[df[i]==37.5,'privatewac'])
    bmn.loc[i,'PRJB46-60']=sum(df.loc[df[i]==52.5,'privatewac'])
    bmn.loc[i,'PRJB61-75']=sum(df.loc[df[i]==67.5,'privatewac'])
    bmn.loc[i,'PRJB76-90']=sum(df.loc[df[i]==82.5,'privatewac'])
    bmn.loc[i,'PRJB91-105']=sum(df.loc[df[i]==97.5,'privatewac'])
    bmn.loc[i,'PRJB106-120']=sum(df.loc[df[i]==112.5,'privatewac'])
    bmn.loc[i,'PUJB1-15']=sum(df.loc[df[i]==7.5,'publicwac'])
    bmn.loc[i,'PUJB16-30']=sum(df.loc[df[i]==22.5,'publicwac'])
    bmn.loc[i,'PUJB31-45']=sum(df.loc[df[i]==37.5,'publicwac'])
    bmn.loc[i,'PUJB46-60']=sum(df.loc[df[i]==52.5,'publicwac'])
    bmn.loc[i,'PUJB61-75']=sum(df.loc[df[i]==67.5,'publicwac'])
    bmn.loc[i,'PUJB76-90']=sum(df.loc[df[i]==82.5,'publicwac'])
    bmn.loc[i,'PUJB91-105']=sum(df.loc[df[i]==97.5,'publicwac'])
    bmn.loc[i,'PUJB106-120']=sum(df.loc[df[i]==112.5,'publicwac'])   
bmn.to_csv(path+'bmn2/bmn2.csv',index=True)