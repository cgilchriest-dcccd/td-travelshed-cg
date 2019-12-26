#! /usr/bin/python3
# Travelshed Mapping
# OpenTripPlanner Guide: http://docs.opentripplanner.org/en/latest/
# OpenTripPlanner API Doc: http://dev.opentripplanner.org/apidoc/1.0.0/index.html

import matplotlib

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
path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2018/TRAVELSHEDREVAMP/'
#path='C:/Users/Y_Ma2/Desktop/amazon/'
#doserver='http://142.93.21.138:8801/'
doserver='http://localhost:8801/'





# Adjustment
adjlist=["36005031900","36005050400","36047016400","36047062800","36047066600","36047070202","36047070203","36061000100",
         "36061000500","36081010701","36081029700","36081029900","36081038302","36081071600","36081091601","36081091602",
         "36081100801","36081148300","36081155101","36085014604","36085014606","36085015400","36085017008","36085017600",
         "36085017700","36085027900","36085990100"]




# Summarize travelshed outputs
# NYC Res Censust Blocks
adjres=pd.DataFrame()
for i in adjlist:
    tp=pd.read_csv(path+'nyctract/res2/ADJRES'+i+'.csv',dtype=str)
    tp=tp.set_index('blockid')
    adjres=pd.concat([adjres,tp],axis=1)
resbk=pd.read_csv(path+'nyctract/resbk.csv',dtype=str)
resbk=resbk.set_index('blockid')
resloclist=resbk.columns
resbk=pd.concat([resbk,adjres],axis=1)
for i in adjlist:
    resbk['RES'+i]=resbk['ADJRES'+i]
resbk=resbk[resloclist]
resbk.to_csv(path+'nyctract/resbk2.csv',index=True)
# NYC Res Censust Tracts
adjresloclist=adjres.columns
for i in adjres.columns:
    adjres[i]=pd.to_numeric(adjres[i])
adjres=adjres.replace(999,np.nan)
adjres['tractid']=[str(x)[0:11] for x in adjres.index]
adjres=adjres.groupby(['tractid'])[adjresloclist].median(skipna=True)
resct=pd.read_csv(path+'nyctract/resct.csv',dtype=str)
resct=resct.set_index('tractid')
resloclist=resct.columns
for i in resct.columns:
    resct[i]=pd.to_numeric(resct[i])
resct=pd.concat([resct,adjres],axis=1)
for i in adjlist:
    resct['RES'+i]=resct['ADJRES'+i]
resct=resct[resloclist]
resct.to_csv(path+'nyctract/resct2.csv',index=True,na_rep='999')





# NYC Work Censust Blocks
adjwork=pd.DataFrame()
for i in adjlist:
    tp=pd.read_csv(path+'nyctract/work2/ADJWORK'+i+'.csv',dtype=str)
    tp=tp.set_index('blockid')
    adjwork=pd.concat([adjwork,tp],axis=1)
workbk=pd.read_csv(path+'nyctract/workbk.csv',dtype=str)
workbk=workbk.set_index('blockid')
workloclist=workbk.columns
workbk=pd.concat([workbk,adjwork],axis=1)
for i in adjlist:
    workbk['WORK'+i]=workbk['ADJWORK'+i]
workbk=workbk[workloclist]
workbk.to_csv(path+'nyctract/workbk2.csv',index=True)
# NYC Work Censust Tracts
adjworkloclist=adjwork.columns
for i in adjwork.columns:
    adjwork[i]=pd.to_numeric(adjwork[i])
adjwork=adjwork.replace(999,np.nan)
adjwork['tractid']=[str(x)[0:11] for x in adjwork.index]
adjwork=adjwork.groupby(['tractid'])[adjworkloclist].median(skipna=True)
workct=pd.read_csv(path+'nyctract/workct.csv',dtype=str)
workct=workct.set_index('tractid')
workloclist=workct.columns
for i in workct.columns:
    workct[i]=pd.to_numeric(workct[i])
workct=pd.concat([workct,adjwork],axis=1)
for i in adjlist:
    workct['WORK'+i]=workct['ADJWORK'+i]
workct=workct[workloclist]
workct.to_csv(path+'nyctract/workct2.csv',index=True,na_rep='999')














# Map checks

# Define add basemap
def add_basemap(ax, zoom, url='http://tile.stamen.com/terrain/tileZ/tileX/tileY.png'):
    xmin, xmax, ymin, ymax = ax.axis()
    basemap, extent = ctx.bounds2img(xmin, ymin, xmax, ymax, zoom=zoom, url=url)
    ax.imshow(basemap, extent=extent, interpolation='bilinear')
    ax.axis((xmin, xmax, ymin, ymax))

# Map adjusted walk and transit
ct=gpd.read_file(path+'shp/quadstatectclipped.shp')
ct.crs={'init': 'epsg:4326'}
ct=ct[['tractid','geometry']]
# Res
resct=pd.read_csv(path+'nyctract/resct2.csv',dtype=str)
for i in resct.columns[1:]:
    resct[i]=pd.to_numeric(resct[i])
resct=ct.merge(resct,on='tractid')
for i in ['RES'+x for x in adjlist]:
    resctmap=resct.loc[resct[i]<=120,[i,'geometry']]
    resctmap=resctmap.to_crs(epsg=3857)
    fig,ax=plt.subplots(1,figsize=(11,8.5))
    plt.subplots_adjust(left=0.05,right=0.95,top=0.95,bottom=0.05)
    ax=resctmap.plot(figsize=(11,8.5),edgecolor=None,column=i,cmap='Spectral',linewidth=0.2,ax=ax,alpha=0.7)
    add_basemap(ax,zoom=11,url=ctx.sources.ST_TONER_LITE)
    ax.set_axis_off()
    sm=plt.cm.ScalarMappable(cmap='Spectral',norm=plt.Normalize(vmin=1,vmax=90))
    sm._A=[]
    divider=mpl_toolkits.axes_grid1.make_axes_locatable(ax)
    cax=divider.append_axes("right",size="3%",pad=0.2,aspect=25)
    cbar=fig.colorbar(sm,cax=cax)
    fig.tight_layout()
    fig.savefig(path+'nyctract/res2/ADJRES'+i+'ct.jpg', dpi=300)
# Work
workct=pd.read_csv(path+'nyctract/workct2.csv',dtype=str)
for i in workct.columns[1:]:
    workct[i]=pd.to_numeric(workct[i])
workct=ct.merge(workct,on='tractid')
for i in ['WORK'+x for x in adjlist]:
    workctmap=workct.loc[workct[i]<=120,[i,'geometry']]
    workctmap=workctmap.to_crs(epsg=3857)
    fig,ax=plt.subplots(1,figsize=(11,8.5))
    plt.subplots_adjust(left=0.05,right=0.95,top=0.95,bottom=0.05)
    ax=workctmap.plot(figsize=(11,8.5),edgecolor=None,column=i,cmap='Spectral',linewidth=0.2,ax=ax,alpha=0.7)
    add_basemap(ax,zoom=11,url=ctx.sources.ST_TONER_LITE)
    ax.set_axis_off()
    sm=plt.cm.ScalarMappable(cmap='Spectral',norm=plt.Normalize(vmin=1,vmax=90))
    sm._A=[]
    divider=mpl_toolkits.axes_grid1.make_axes_locatable(ax)
    cax=divider.append_axes("right",size="3%",pad=0.2,aspect=25)
    cbar=fig.colorbar(sm,cax=cax)
    fig.tight_layout()
    fig.savefig(path+'nyctract/work2/ADJWORK'+i+'ct.jpg', dpi=300)

















# Block Level Gravity Model
# Res Gravity
adjresbkwac=pd.DataFrame()
for i in adjlist:
    tp=pd.read_csv(path+'nyctract/res2/ADJRES'+i+'.csv',dtype=str)
    tp=tp.set_index('blockid')
    adjresbkwac=pd.concat([adjresbkwac,tp],axis=1)
adjresloclist=adjresbkwac.columns
wac=pd.DataFrame()
for i in ['ct','nj','ny','pa']:
    tp=pd.read_csv(path+'lehd/'+str(i)+'_wac_S000_JT03_2015.csv',dtype=str)
    tp=tp[['w_geocode','C000']]
    wac=pd.concat([wac,tp],axis=0)
wac.columns=['blockid','wac']
wac=wac.set_index('blockid')
adjresbkwac=pd.merge(adjresbkwac,wac,how='left',left_index=True,right_index=True)
adjresbkwac['wac']=adjresbkwac['wac'].replace(np.nan,'0')
for i in adjresbkwac.columns:
    adjresbkwac[i]=pd.to_numeric(adjresbkwac[i])
for i in adjresloclist:
    adjresbkwac[i]=np.where(adjresbkwac[i]<=10,5,
                   np.where(adjresbkwac[i]<=20,15,
                   np.where(adjresbkwac[i]<=30,25,
                   np.where(adjresbkwac[i]<=40,35,
                   np.where(adjresbkwac[i]<=50,45,
                   np.where(adjresbkwac[i]<=60,55,
                   np.nan))))))
adjresbkgravity=pd.DataFrame(index=adjresloclist,columns=['WAC1-10','WAC11-20','WAC21-30','WAC31-40','WAC41-50','WAC51-60',
                                                          'GWAC1-10','GWAC11-20','GWAC21-30','GWAC31-40','GWAC41-50','GWAC51-60',
                                                          'GRAVITYWAC'])
for i in adjresloclist:
    tp=sum(adjresbkwac.loc[adjresbkwac[i]==5,'wac'])
    adjresbkgravity.loc[i,'WAC1-10']=tp
    tp=sum(adjresbkwac.loc[adjresbkwac[i]==15,'wac'])
    adjresbkgravity.loc[i,'WAC11-20']=tp
    tp=sum(adjresbkwac.loc[adjresbkwac[i]==25,'wac'])
    adjresbkgravity.loc[i,'WAC21-30']=tp
    tp=sum(adjresbkwac.loc[adjresbkwac[i]==35,'wac'])
    adjresbkgravity.loc[i,'WAC31-40']=tp
    tp=sum(adjresbkwac.loc[adjresbkwac[i]==45,'wac'])
    adjresbkgravity.loc[i,'WAC41-50']=tp
    tp=sum(adjresbkwac.loc[adjresbkwac[i]==55,'wac'])
    adjresbkgravity.loc[i,'WAC51-60']=tp
    adjresbkgravity.loc[i,'GWAC1-10']=(adjresbkgravity.loc[i,'WAC1-10'])/(5**2)
    adjresbkgravity.loc[i,'GWAC11-20']=(adjresbkgravity.loc[i,'WAC11-20'])/(15**2)
    adjresbkgravity.loc[i,'GWAC21-30']=(adjresbkgravity.loc[i,'WAC21-30'])/(25**2)
    adjresbkgravity.loc[i,'GWAC31-40']=(adjresbkgravity.loc[i,'WAC31-40'])/(35**2)
    adjresbkgravity.loc[i,'GWAC41-50']=(adjresbkgravity.loc[i,'WAC41-50'])/(45**2)
    adjresbkgravity.loc[i,'GWAC51-60']=(adjresbkgravity.loc[i,'WAC51-60'])/(55**2)
    adjresbkgravity.loc[i,'GRAVITYWAC']=adjresbkgravity.loc[i,'GWAC1-10']+adjresbkgravity.loc[i,'GWAC11-20']+adjresbkgravity.loc[i,'GWAC21-30']+adjresbkgravity.loc[i,'GWAC31-40']+adjresbkgravity.loc[i,'GWAC41-50']+adjresbkgravity.loc[i,'GWAC51-60']
resbkgravity=pd.read_csv(path+'nyctract/resbkgravity.csv',dtype=str)
resbkgravity=resbkgravity.set_index('Unnamed: 0')
for i in resbkgravity.columns:
    resbkgravity[i]=pd.to_numeric(resbkgravity[i])
for i in adjlist:
    resbkgravity.loc['RES'+i,:]=adjresbkgravity.loc['ADJRES'+i,:]
resbkgravity.to_csv(path+'nyctract/resbkgravity2.csv',index=True)





# Work Gravity
adjworkbkrac=pd.DataFrame()
for i in adjlist:
    tp=pd.read_csv(path+'nyctract/work2/ADJWORK'+i+'.csv',dtype=str)
    tp=tp.set_index('blockid')
    adjworkbkrac=pd.concat([adjworkbkrac,tp],axis=1)
adjworkloclist=adjworkbkrac.columns
rac=pd.DataFrame()
for i in ['ct','nj','ny','pa']:
    tp=pd.read_csv(path+'lehd/'+str(i)+'_rac_S000_JT03_2015.csv',dtype=str)
    tp=tp[['h_geocode','C000']]
    rac=pd.concat([rac,tp],axis=0)
rac.columns=['blockid','rac']
rac=rac.set_index('blockid')
adjworkbkrac=pd.merge(adjworkbkrac,rac,how='left',left_index=True,right_index=True)
adjworkbkrac['rac']=adjworkbkrac['rac'].replace(np.nan,'0')
for i in adjworkbkrac.columns:
    adjworkbkrac[i]=pd.to_numeric(adjworkbkrac[i])
for i in adjworkloclist:
    adjworkbkrac[i]=np.where(adjworkbkrac[i]<=10,5,
                    np.where(adjworkbkrac[i]<=20,15,
                    np.where(adjworkbkrac[i]<=30,25,
                    np.where(adjworkbkrac[i]<=40,35,
                    np.where(adjworkbkrac[i]<=50,45,
                    np.where(adjworkbkrac[i]<=60,55,
                    np.nan))))))
adjworkbkgravity=pd.DataFrame(index=adjworkloclist,columns=['RAC1-10','RAC11-20','RAC21-30','RAC31-40','RAC41-50','RAC51-60',
                                                            'GRAC1-10','GRAC11-20','GRAC21-30','GRAC31-40','GRAC41-50','GRAC51-60',
                                                            'GRAVITYRAC'])
for i in adjworkloclist:
    tp=sum(adjworkbkrac.loc[adjworkbkrac[i]==5,'rac'])
    adjworkbkgravity.loc[i,'RAC1-10']=tp
    tp=sum(adjworkbkrac.loc[adjworkbkrac[i]==15,'rac'])
    adjworkbkgravity.loc[i,'RAC11-20']=tp
    tp=sum(adjworkbkrac.loc[adjworkbkrac[i]==25,'rac'])
    adjworkbkgravity.loc[i,'RAC21-30']=tp
    tp=sum(adjworkbkrac.loc[adjworkbkrac[i]==35,'rac'])
    adjworkbkgravity.loc[i,'RAC31-40']=tp
    tp=sum(adjworkbkrac.loc[adjworkbkrac[i]==45,'rac'])
    adjworkbkgravity.loc[i,'RAC41-50']=tp
    tp=sum(adjworkbkrac.loc[adjworkbkrac[i]==55,'rac'])
    adjworkbkgravity.loc[i,'RAC51-60']=tp
    adjworkbkgravity.loc[i,'GRAC1-10']=(adjworkbkgravity.loc[i,'RAC1-10'])/(5**2)
    adjworkbkgravity.loc[i,'GRAC11-20']=(adjworkbkgravity.loc[i,'RAC11-20'])/(15**2)
    adjworkbkgravity.loc[i,'GRAC21-30']=(adjworkbkgravity.loc[i,'RAC21-30'])/(25**2)
    adjworkbkgravity.loc[i,'GRAC31-40']=(adjworkbkgravity.loc[i,'RAC31-40'])/(35**2)
    adjworkbkgravity.loc[i,'GRAC41-50']=(adjworkbkgravity.loc[i,'RAC41-50'])/(45**2)
    adjworkbkgravity.loc[i,'GRAC51-60']=(adjworkbkgravity.loc[i,'RAC51-60'])/(55**2)
    adjworkbkgravity.loc[i,'GRAVITYRAC']=adjworkbkgravity.loc[i,'GRAC1-10']+adjworkbkgravity.loc[i,'GRAC11-20']+adjworkbkgravity.loc[i,'GRAC21-30']+adjworkbkgravity.loc[i,'GRAC31-40']+adjworkbkgravity.loc[i,'GRAC41-50']+adjworkbkgravity.loc[i,'GRAC51-60']
workbkgravity=pd.read_csv(path+'nyctract/workbkgravity.csv',dtype=str)
workbkgravity=workbkgravity.set_index('Unnamed: 0')
for i in workbkgravity.columns:
    workbkgravity[i]=pd.to_numeric(workbkgravity[i])
for i in adjlist:
    workbkgravity.loc['WORK'+i,:]=adjworkbkgravity.loc['ADJWORK'+i,:]
workbkgravity.to_csv(path+'nyctract/workbkgravity2.csv',index=True)







## Tract Level Gravity Model
# Res Gravity
resctwac=pd.read_csv(path+'nyctract/resct2.csv',dtype=str)
resctwac=resctwac.set_index('tractid')
resloclist=sorted(resctwac.columns)
wac=pd.DataFrame()
for i in ['ct','nj','ny','pa']:
    tp=pd.read_csv(path+'lehd/'+str(i)+'_wac_S000_JT03_2015.csv',dtype=str)
    tp=tp[['w_geocode','C000']]
    wac=pd.concat([wac,tp],axis=0)
wac.columns=['blockid','wac']
wac['tractid']=[str(x)[0:11] for x in wac['blockid']]
wac['wac']=pd.to_numeric(wac['wac'])
wac=pd.DataFrame(wac.groupby('tractid')['wac'].sum())
resctwac=pd.merge(resctwac,wac,how='left',left_index=True,right_index=True)
resctwac['wac']=resctwac['wac'].replace(np.nan,'0')
for i in resctwac.columns:
    resctwac[i]=pd.to_numeric(resctwac[i])
for i in resloclist:
    resctwac[i]=np.where(resctwac[i]<=10,5,
                np.where(resctwac[i]<=20,15,
                np.where(resctwac[i]<=30,25,
                np.where(resctwac[i]<=40,35,
                np.where(resctwac[i]<=50,45,
                np.where(resctwac[i]<=60,55,
                np.nan))))))
resctgravity=pd.DataFrame(index=resloclist,columns=['WAC1-10','WAC11-20','WAC21-30','WAC31-40','WAC41-50','WAC51-60',
                                                    'GWAC1-10','GWAC11-20','GWAC21-30','GWAC31-40','GWAC41-50','GWAC51-60',
                                                    'GRAVITYWAC'])
for i in resloclist:
    tp=sum(resctwac.loc[resctwac[i]==5,'wac'])
    resctgravity.loc[i,'WAC1-10']=tp
    tp=sum(resctwac.loc[resctwac[i]==15,'wac'])
    resctgravity.loc[i,'WAC11-20']=tp
    tp=sum(resctwac.loc[resctwac[i]==25,'wac'])
    resctgravity.loc[i,'WAC21-30']=tp
    tp=sum(resctwac.loc[resctwac[i]==35,'wac'])
    resctgravity.loc[i,'WAC31-40']=tp
    tp=sum(resctwac.loc[resctwac[i]==45,'wac'])
    resctgravity.loc[i,'WAC41-50']=tp
    tp=sum(resctwac.loc[resctwac[i]==55,'wac'])
    resctgravity.loc[i,'WAC51-60']=tp
    resctgravity.loc[i,'GWAC1-10']=(resctgravity.loc[i,'WAC1-10'])/(5**2)
    resctgravity.loc[i,'GWAC11-20']=(resctgravity.loc[i,'WAC11-20'])/(15**2)
    resctgravity.loc[i,'GWAC21-30']=(resctgravity.loc[i,'WAC21-30'])/(25**2)
    resctgravity.loc[i,'GWAC31-40']=(resctgravity.loc[i,'WAC31-40'])/(35**2)
    resctgravity.loc[i,'GWAC41-50']=(resctgravity.loc[i,'WAC41-50'])/(45**2)
    resctgravity.loc[i,'GWAC51-60']=(resctgravity.loc[i,'WAC51-60'])/(55**2)
    resctgravity.loc[i,'GRAVITYWAC']=resctgravity.loc[i,'GWAC1-10']+resctgravity.loc[i,'GWAC11-20']+resctgravity.loc[i,'GWAC21-30']+resctgravity.loc[i,'GWAC31-40']+resctgravity.loc[i,'GWAC41-50']+resctgravity.loc[i,'GWAC51-60']
resctgravity.to_csv(path+'nyctract/resctgravity2.csv',index=True)

# Work Gravity
workctrac=pd.read_csv(path+'nyctract/workct2.csv',dtype=str)
workctrac=workctrac.set_index('tractid')
workloclist=sorted(workctrac.columns)
rac=pd.DataFrame()
for i in ['ct','nj','ny','pa']:
    tp=pd.read_csv(path+'lehd/'+str(i)+'_rac_S000_JT03_2015.csv',dtype=str)
    tp=tp[['h_geocode','C000']]
    rac=pd.concat([rac,tp],axis=0)
rac.columns=['blockid','rac']
rac['tractid']=[str(x)[0:11] for x in rac['blockid']]
rac['rac']=pd.to_numeric(rac['rac'])
rac=pd.DataFrame(rac.groupby('tractid')['rac'].sum())
workctrac=pd.merge(workctrac,rac,how='left',left_index=True,right_index=True)
workctrac['rac']=workctrac['rac'].replace(np.nan,'0')
for i in workctrac.columns:
    workctrac[i]=pd.to_numeric(workctrac[i])
for i in workloclist:
    workctrac[i]=np.where(workctrac[i]<=10,5,
                 np.where(workctrac[i]<=20,15,
                 np.where(workctrac[i]<=30,25,
                 np.where(workctrac[i]<=40,35,
                 np.where(workctrac[i]<=50,45,
                 np.where(workctrac[i]<=60,55,
                 np.nan))))))
workctgravity=pd.DataFrame(index=workloclist,columns=['RAC1-10','RAC11-20','RAC21-30','RAC31-40','RAC41-50','RAC51-60',
                                                      'GRAC1-10','GRAC11-20','GRAC21-30','GRAC31-40','GRAC41-50','GRAC51-60',
                                                      'GRAVITYRAC'])
for i in workloclist:
    tp=sum(workctrac.loc[workctrac[i]==5,'rac'])
    workctgravity.loc[i,'RAC1-10']=tp
    tp=sum(workctrac.loc[workctrac[i]==15,'rac'])
    workctgravity.loc[i,'RAC11-20']=tp
    tp=sum(workctrac.loc[workctrac[i]==25,'rac'])
    workctgravity.loc[i,'RAC21-30']=tp
    tp=sum(workctrac.loc[workctrac[i]==35,'rac'])
    workctgravity.loc[i,'RAC31-40']=tp
    tp=sum(workctrac.loc[workctrac[i]==45,'rac'])
    workctgravity.loc[i,'RAC41-50']=tp
    tp=sum(workctrac.loc[workctrac[i]==55,'rac'])
    workctgravity.loc[i,'RAC51-60']=tp
    workctgravity.loc[i,'GRAC1-10']=(workctgravity.loc[i,'RAC1-10'])/(5**2)
    workctgravity.loc[i,'GRAC11-20']=(workctgravity.loc[i,'RAC11-20'])/(15**2)
    workctgravity.loc[i,'GRAC21-30']=(workctgravity.loc[i,'RAC21-30'])/(25**2)
    workctgravity.loc[i,'GRAC31-40']=(workctgravity.loc[i,'RAC31-40'])/(35**2)
    workctgravity.loc[i,'GRAC41-50']=(workctgravity.loc[i,'RAC41-50'])/(45**2)
    workctgravity.loc[i,'GRAC51-60']=(workctgravity.loc[i,'RAC51-60'])/(55**2)
    workctgravity.loc[i,'GRAVITYRAC']=workctgravity.loc[i,'GRAC1-10']+workctgravity.loc[i,'GRAC11-20']+workctgravity.loc[i,'GRAC21-30']+workctgravity.loc[i,'GRAC31-40']+workctgravity.loc[i,'GRAC41-50']+workctgravity.loc[i,'GRAC51-60']
workctgravity.to_csv(path+'nyctract/workctgravity2.csv',index=True)













