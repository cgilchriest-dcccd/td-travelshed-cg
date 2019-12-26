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
#path='C:/Users/Y_Ma2/Desktop/amazon/'
#path='C:/Users/Y_Ma2/Desktop/TEST/'
#path='E:/TRAVELSHEDREVAMP/'
#doserver='http://142.93.21.138:8801/'
#doserver='http://159.65.64.166:8801/'
doserver='http://localhost:8801/'




## Map checks
#
# Define add basemap
def add_basemap(ax, zoom, url='http://tile.stamen.com/terrain/tileZ/tileX/tileY.png'):
    xmin, xmax, ymin, ymax = ax.axis()
    basemap, extent = ctx.bounds2img(xmin, ymin, xmax, ymax, zoom=zoom, url=url)
    ax.imshow(basemap, extent=extent, interpolation='bilinear')
    ax.axis((xmin, xmax, ymin, ymax))

## Map tract level
#ct=gpd.read_file(path+'shp/quadstatectclipped.shp')
#ct.crs={'init': 'epsg:4326'}
#ct=ct[['tractid','geometry']]
## Res
#resct=pd.read_csv(path+'nyctract/resct2.csv',dtype=float,converters={'tractid':str})
#resloclist=resct.columns[1:]
#resct=ct.merge(resct,on='tractid')
#for i in resloclist:
#    resctmap=resct.loc[resct[i]<=120,[i,'geometry']]
#    resctmap=resctmap.to_crs(epsg=3857)
#    fig,ax=plt.subplots(1,figsize=(11,8.5))
#    plt.subplots_adjust(left=0.05,right=0.95,top=0.95,bottom=0.05)
#    ax=resctmap.plot(figsize=(11,8.5),edgecolor=None,column=i,cmap='Spectral',linewidth=0.2,ax=ax,alpha=0.7)
#    add_basemap(ax,zoom=11,url=ctx.sources.ST_TONER_LITE)
#    ax.set_axis_off()
#    ax.set_title('AM Peak Transit Travel Time (Minutes) from '+i,fontdict={'fontsize':'16','fontweight':'10'})
#    sm=plt.cm.ScalarMappable(cmap='Spectral',norm=plt.Normalize(vmin=1,vmax=120))
#    sm._A=[]
#    divider=mpl_toolkits.axes_grid1.make_axes_locatable(ax)
#    cax=divider.append_axes("right",size="3%",pad=0.2,aspect=25)
#    cbar=fig.colorbar(sm,cax=cax)
#    fig.tight_layout()
#    fig.savefig(path+'nyctract/res3/'+i+'ct.jpg', dpi=300)
## Work
#workct=pd.read_csv(path+'nyctract/workct2.csv',dtype=float,converters={'tractid':str})
#workloclist=workct.columns[1:]
#workct=ct.merge(workct,on='tractid')
#for i in workloclist:
#    workctmap=workct.loc[workct[i]<=120,[i,'geometry']]
#    workctmap=workctmap.to_crs(epsg=3857)
#    fig,ax=plt.subplots(1,figsize=(11,8.5))
#    plt.subplots_adjust(left=0.05,right=0.95,top=0.95,bottom=0.05)
#    ax=workctmap.plot(figsize=(11,8.5),edgecolor=None,column=i,cmap='Spectral',linewidth=0.2,ax=ax,alpha=0.7)
#    add_basemap(ax,zoom=11,url=ctx.sources.ST_TONER_LITE)
#    ax.set_axis_off()
#    sm=plt.cm.ScalarMappable(cmap='Spectral',norm=plt.Normalize(vmin=1,vmax=120))
#    sm._A=[]
#    divider=mpl_toolkits.axes_grid1.make_axes_locatable(ax)
#    cax=divider.append_axes("right",size="3%",pad=0.2,aspect=25)
#    cbar=fig.colorbar(sm,cax=cax)
#    fig.tight_layout()
#    fig.savefig(path+'nyctract/work3/'+i+'ct.jpg', dpi=300)




















# Adjustment
#adjlist=['36005000200','36005000400','36005002400','36005003300','36005003500','36005003700','36005003800','36005003900',
#         '36005004001','36005004100','36005004300','36005004400','36005005100','36005005300','36005006500','36005006700',
#         '36005007400','36005007500','36005007800','36005007900','36005008500','36005009000','36005013100','36005013300',
#         '36005015300','36005016500','36005018302','36005020400','36005021002','36005021602','36005022403','36005022404',
#         '36005022702','36005022800','36005022902','36005023200','36005024600','36005024700','36005025600','36005026400',
#         '36005027402','36005027700','36005028100','36005028800','36005030900','36005031400','36005031600','36005031800',
#         '36005032400','36005032600','36005032800','36005033000','36005033202','36005034000','36005034200','36005034400',
#         '36005034500','36005034800','36005035100','36005035600','36005035800','36005035900','36005036000','36005036400',
#         '36005036700','36005037200','36005037600','36005038000','36005038302','36005038500','36005038700','36005038800',
#         '36005039000','36005039100','36005039300','36005039400','36005039600','36005039800','36005040400','36005040600',
#         '36005040800','36005041800','36005042000','36005042200','36005042400','36005042800','36005043000','36005043100',
#         '36005043500','36005044200','36005045600','36005046202','36005048400','36005050400']

#adjlist=['36047000301','36047000700','36047001500','36047001800','36047002200','36047003100','36047003900','36047004600',
#         '36047004900','36047005900','36047006300','36047006700','36047007100','36047008200','36047009400','36047009600',
#         '36047009800','36047010000','36047010200','36047012200','36047012700','36047013000','36047013100','36047013400',
#         '36047013600','36047014200','36047014500','36047014700','36047015900','36047016300','36047016500','36047016600',
#         '36047017500','36047017700','36047018000','36047018400','36047019400','36047019700','36047020500','36047020700',
#         '36047021000','36047021300','36047021500','36047021900','36047022100','36047023100','36047023200','36047024900',
#         '36047025500','36047025700','36047025902','36047026600','36047026700','36047026900','36047027500','36047029100',
#         '36047029500','36047030300','36047030500','36047030700','36047030900','36047031100','36047031300','36047031500',
#         '36047031701','36047032300','36047032500','36047033100','36047033300','36047034300','36047034500','36047034700',
#         '36047034900','36047035500','36047035700','36047036100','36047036501','36047036600','36047037700','36047038100',
#         '36047040600','36047041300','36047041500','36047041800','36047042000','36047042100','36047042300','36047042500',
#         '36047043100','36047043500','36047044200','36047044800','36047044900','36047046800','36047047000','36047048400',
#         '36047048800','36047049000','36047050500','36047051500','36047052300','36047052600','36047054200','36047054400',
#         '36047054500','36047054700','36047054900','36047055600','36047060000','36047062800','36047064600','36047064800',
#         '36047065800','36047067400','36047069602','36047070600','36047076800','36047077600','36047078200','36047078800',
#         '36047079200','36047079400','36047079601','36047079602','36047079802','36047080000','36047080200','36047080600',
#         '36047081000','36047081400','36047081600','36047081800','36047082200','36047083600','36047084800','36047085000',
#         '36047085200','36047085400','36047085600','36047085800','36047086000','36047086600','36047087401','36047087600',
#         '36047088200','36047088400','36047089000','36047089600','36047090200','36047091000','36047091600','36047091800',
#         '36047092000','36047092400','36047092800','36047093200','36047094401','36047095800','36047096000','36047097000',
#         '36047098400','36047098600','36047102600','36047105804','36047110400','36047111600','36047112200','36047114201',
#         '36047116000','36047116200','36047116400','36047116800','36047117201','36047117601','36047117800','36047118202',
#         '36047118600','36047119000','36047119200','36047119800','36047120800','36047121000','36047121400','36047150200']

adjlist=['36061001200','36061001501','36061001502','36061002500','36061002700','36061002900','36061003300','36061003601',
         '36061003900','36061004500','36061004700','36061004900','36061005200','36061005900','36061006000','36061006100',
         '36061006800','36061006900','36061007500','36061007900','36061008200','36061008400','36061008602','36061008603',
         '36061009000','36061009200','36061009600','36061009700','36061009900','36061010200','36061010601','36061010602',
         '36061011100','36061011800','36061012000','36061012400','36061012500','36061012800','36061012900','36061013200',
         '36061013400','36061013600','36061013800','36061013900','36061014300','36061014401','36061014402','36061014700',
         '36061014802','36061015001','36061015002','36061015500','36061015801','36061016100','36061016200','36061017100',
         '36061017402','36061017700','36061017900','36061018400','36061018700','36061019000','36061019400','36061019701',
         '36061019800','36061020000','36061020101','36061020600','36061020800','36061021000','36061021303','36061021400',
         '36061021500','36061021600','36061022000','36061022302','36061022400','36061022600','36061023000','36061023100',
         '36061023200','36061023502','36061023802','36061024000','36061024200','36061024700','36061024900','36061025500',
         '36061026100','36061026700','36061026900','36061027500','36061028100','36061028300','36061029100','36061029500',
         '36061029700','36061030700','36061031100','36061031703','36061031704']

#adjlist=['36085001800','36085002001','36085003900','36085004000','36085005900','36085007700','36085009602','36085011201',
#         '36085011401','36085011402','36085012805','36085013800','36085014604','36085014606','36085014607','36085014700',
#         '36085015100','36085015601','36085017007','36085017009','36085017010','36085017700','36085018100','36085018702',
#         '36085020803','36085022600','36085032300']



# Load quadstate blokc point shapefile
bkpt=gpd.read_file(path+'shp/quadstatebkpt.shp')
bkpt.crs={'init': 'epsg:4326'}

# Set typical day
typicaldate='2018/06/06'

# Create arrival time list
arrivaltimeinterval=10 # in minutes
arrivaltimestart='07:00:00'
arrivaltimeend='10:00:00'
arrivaltimestart=datetime.datetime.strptime(arrivaltimestart,'%H:%M:%S')
arrivaltimeend=datetime.datetime.strptime(arrivaltimeend,'%H:%M:%S')
arrivaltimeincrement=arrivaltimestart
arrivaltime=[]
while arrivaltimeincrement<=arrivaltimeend:
    arrivaltime.append(datetime.datetime.strftime(arrivaltimeincrement,'%H:%M:%S'))
    arrivaltimeincrement+=datetime.timedelta(seconds=arrivaltimeinterval*60)

# Set maximum number of transfers
maxTransfers=3 # 4 boardings

# Set maximum walking distance
maxWalkDistance=1000 # in meters

# Set cut off points between 0-120 mins
cutoffinterval=2 # in minutes
cutoffstart=0
cutoffend=120
cutoffincrement=cutoffstart
cutoff=''
while cutoffincrement<cutoffend:
    cutoff+='&cutoffSec='+str((cutoffincrement+cutoffinterval)*60)
    cutoffincrement+=cutoffinterval

# Definie res travelshed function to generate isochrones and spatial join to Census Blocks
def restravelshed(arrt):
    bk=bkpt.copy()
    url='http://localhost:8801/otp/routers/default/isochrone?batch=true&mode=WALK,TRANSIT'
    url+='&fromPlace='+destination.loc[i,'latlong']
    url+='&date='+typicaldate+'&time='+arrt+'&maxTransfers='+str(maxTransfers)
    url+='&maxWalkDistance='+str(maxWalkDistance)+'&clampInitialWait=0'+cutoff
    headers={'Accept':'application/json'}  
    req=requests.get(url=url,headers=headers)
    js=req.json()
    iso=gpd.GeoDataFrame.from_features(js,crs={'init': 'epsg:4326'})
    bk['T'+arrt[0:2]+arrt[3:5]]=999
    cut=range(cutoffend,cutoffstart,-cutoffinterval)
    bkiso=gpd.sjoin(bk,iso.loc[iso['time']==cut[0]*60],how='left',op='within')
    bkiso=bkiso.loc[pd.notnull(bkiso['time']),'blockid']
    bk.loc[bk['blockid'].isin(bkiso),'T'+arrt[0:2]+arrt[3:5]]=cut[0]-cutoffinterval/2
    for k in range(0,(len(cut)-1)):
        if (iso.loc[iso['time']==cut[k+1]*60,'geometry'].notna()).bool():
            if len(bk.loc[bk['T'+arrt[0:2]+arrt[3:5]]==cut[k]-cutoffinterval/2])!=0:
                try:
                    bkiso=gpd.sjoin(bk.loc[bk['T'+arrt[0:2]+arrt[3:5]]==cut[k]-cutoffinterval/2],
                                    iso.loc[iso['time']==cut[k+1]*60],how='left',op='within')
                    bkiso=bkiso.loc[pd.notnull(bkiso['time']),'blockid']
                    bk.loc[bk['blockid'].isin(bkiso),'T'+arrt[0:2]+arrt[3:5]]=cut[k+1]-cutoffinterval/2
                except ValueError:
                    print(destination.loc[i,'id']+' '+arrt+' '+
                          str(cut[k+1])+'-minute isochrone has no Census Block in it!')
            else:
                print(destination.loc[i,'id']+' '+arrt+' '+
                      str(cut[k])+'-minute isochrone has no Census Block in it!')
        else:
            print(destination.loc[i,'id']+' '+arrt+' '+
                  str(cut[k+1])+'-minute isochrone has no geometry!')
    bk['T'+arrt[0:2]+arrt[3:5]]=bk['T'+arrt[0:2]+arrt[3:5]].replace(999,np.nan)
    bk=bk.drop(['lat','long','geometry'],axis=1)
    bk=bk.set_index('blockid')
    return bk

# Definie work travelshed function to generate isochrones and spatial join to Census Blocks
def worktravelshed(arrt):
    bk=bkpt.copy()
    url='http://localhost:8801/otp/routers/default/isochrone?batch=true&mode=WALK,TRANSIT'
    url+='&fromPlace='+destination.loc[i,'latlong']+'&toPlace='+destination.loc[i,'latlong']
    url+='&arriveBy=true&date='+typicaldate+'&time='+arrt+'&maxTransfers='+str(maxTransfers)
    url+='&maxWalkDistance='+str(maxWalkDistance)+'&clampInitialWait=-1'+cutoff
    headers={'Accept':'application/json'}  
    req=requests.get(url=url,headers=headers)
    js=req.json()
    iso=gpd.GeoDataFrame.from_features(js,crs={'init': 'epsg:4326'})
    bk['T'+arrt[0:2]+arrt[3:5]]=999
    cut=range(cutoffend,cutoffstart,-cutoffinterval)
    bkiso=gpd.sjoin(bk,iso.loc[iso['time']==cut[0]*60],how='left',op='within')
    bkiso=bkiso.loc[pd.notnull(bkiso['time']),'blockid']
    bk.loc[bk['blockid'].isin(bkiso),'T'+arrt[0:2]+arrt[3:5]]=cut[0]-cutoffinterval/2
    for k in range(0,(len(cut)-1)):
        if (iso.loc[iso['time']==cut[k+1]*60,'geometry'].notna()).bool():
            if len(bk.loc[bk['T'+arrt[0:2]+arrt[3:5]]==cut[k]-cutoffinterval/2])!=0:
                try:
                    bkiso=gpd.sjoin(bk.loc[bk['T'+arrt[0:2]+arrt[3:5]]==cut[k]-cutoffinterval/2],
                                    iso.loc[iso['time']==cut[k+1]*60],how='left',op='within')
                    bkiso=bkiso.loc[pd.notnull(bkiso['time']),'blockid']
                    bk.loc[bk['blockid'].isin(bkiso),'T'+arrt[0:2]+arrt[3:5]]=cut[k+1]-cutoffinterval/2
                except ValueError:
                    print(destination.loc[i,'id']+' '+arrt+' '+
                          str(cut[k+1])+'-minute isochrone has no Census Block in it!')
            else:
                print(destination.loc[i,'id']+' '+arrt+' '+
                      str(cut[k])+'-minute isochrone has no Census Block in it!')
        else:
            print(destination.loc[i,'id']+' '+arrt+' '+
                  str(cut[k+1])+'-minute isochrone has no geometry!')
    bk['T'+arrt[0:2]+arrt[3:5]]=bk['T'+arrt[0:2]+arrt[3:5]].replace(999,np.nan)
    bk=bk.drop(['lat','long','geometry'],axis=1)
    bk=bk.set_index('blockid')
    return bk


# Define parallel multiprocessing function
def parallelize(data, func):
    data_split=np.array_split(data,np.ceil(len(data)/(mp.cpu_count()-4)))
    pool=mp.Pool(mp.cpu_count()-4)
    dt=pd.DataFrame()
    for i in data_split:
        ds=pd.concat(pool.map(func,i),axis=1)
        dt=pd.concat([dt,ds],axis=1)
    pool.close()
    pool.join()
    return dt


if __name__=='__main__':
    location=pd.read_excel(path+'nyctract/centroid/centroid.xlsx',sheet_name='nycrestractptadjfinal',dtype=str)
    location=location[np.isin(location['censustract'],adjlist)].reset_index(drop=True)
    location['id']=['ADJRES'+str(x).zfill(11) for x in location['censustract']]
    location['latlong']=[str(x)+','+str(y) for x,y in zip(location['resintlatfinal'],location['resintlongfinal'])]
    destination=location.loc[0:max(location.count())-1,['id','latlong']]
    for i in destination.index:
        df=parallelize(arrivaltime,restravelshed)
        df['TTMEDIAN']=df.median(skipna=True,axis=1)
        df=df['TTMEDIAN'].sort_index()
        df.name=destination.loc[i,'id']
        df.to_csv(path+'nyctract/res3/'+destination.loc[i,'id']+'.csv',index=True,header=True,na_rep=999)
    # Join travelsheds to block shapefile
    wtbk=gpd.read_file(path+'shp/quadstatebkclipped.shp')
    wtbk.crs={'init': 'epsg:4326'}
    wtbk=wtbk[['blockid','geometry']]    
    for i in destination.index:
        tp=pd.read_csv(path+'nyctract/res3/'+destination.loc[i,'id']+'.csv',dtype=str)
        tp.iloc[:,1]=pd.to_numeric(tp.iloc[:,1])
        wtbk=wtbk.merge(tp,on='blockid')
    # Join travelsheds to tract shapefile
    wtbk=wtbk.replace(999,np.nan)
    loclist=wtbk.columns[1:]
    wtbk['tractid']=[str(x)[0:11] for x in wtbk['blockid']]
    wtbk=wtbk.groupby(['tractid'])[loclist].median(skipna=True)
    wtbk=wtbk.replace(np.nan,999)
    wtbk=wtbk.reset_index()
    wtct=gpd.read_file(path+'shp/quadstatectclipped.shp')
    wtct.crs={'init': 'epsg:4326'}
    wtct=wtct[['tractid','geometry']]
    wtct=wtct.merge(wtbk,on='tractid')
    for i in destination.index:
        # Create tract level map
        wtctmap=wtct.loc[wtct[destination.loc[i,'id']]<=120,[destination.loc[i,'id'],'geometry']]
        wtctmap=wtctmap.to_crs(epsg=3857)
        fig,ax=plt.subplots(1,figsize=(11,8.5))
        plt.subplots_adjust(left=0.05,right=0.95,top=0.95,bottom=0.05)
        ax=wtctmap.plot(figsize=(11,8.5),edgecolor=None,column=destination.loc[i,'id'],cmap='Spectral',linewidth=0.2,ax=ax,alpha=0.7)
        add_basemap(ax,zoom=11,url=ctx.sources.ST_TONER_LITE)
        ax.set_axis_off()
        ax.set_title('AM Peak Transit Travel Time (Minutes) from '+destination.loc[i,'id'],fontdict={'fontsize':'16','fontweight':'10'})
        sm=plt.cm.ScalarMappable(cmap='Spectral',norm=plt.Normalize(vmin=1,vmax=120))
        sm._A=[]
        divider=mpl_toolkits.axes_grid1.make_axes_locatable(ax)
        cax=divider.append_axes("right",size="3%",pad=0.2,aspect=25)
        cbar=fig.colorbar(sm,cax=cax)
        fig.tight_layout()
        fig.savefig(path+'nyctract/res3/'+destination.loc[i,'id']+'ct.jpg', dpi=300)
    print(datetime.datetime.now()-start)





## Summarize travelshed outputs
## NYC Res Censust Blocks
#adjres=pd.DataFrame()
#for i in adjlist:
#    tp=pd.read_csv(path+'nyctract/res/ADJRES'+i+'.csv',dtype=str)
#    tp=tp.set_index('blockid')
#    adjres=pd.concat([adjres,tp],axis=1)
#resbk=pd.read_csv(path+'nyctract/resbk.csv',dtype=str)
#resbk=resbk.set_index('blockid')
#resloclist=resbk.columns
#resbk=pd.concat([resbk,adjres],axis=1)
#for i in adjlist:
#    resbk['RES'+i]=resbk['ADJRES'+i]
#resbk=resbk[resloclist]
#resbk.to_csv(path+'nyctract/resbk2.csv',index=True)
## NYC Res Censust Tracts
#adjresloclist=adjres.columns
#for i in adjres.columns:
#    adjres[i]=pd.to_numeric(adjres[i])
#adjres=adjres.replace(999,np.nan)
#adjres['tractid']=[str(x)[0:11] for x in adjres.index]
#adjres=adjres.groupby(['tractid'])[adjresloclist].median(skipna=True)
#resct=pd.read_csv(path+'nyctract/resct.csv',dtype=str)
#resct=resct.set_index('tractid')
#resloclist=resct.columns
#for i in resct.columns:
#    resct[i]=pd.to_numeric(resct[i])
#resct=pd.concat([resct,adjres],axis=1)
#for i in adjlist:
#    resct['RES'+i]=resct['ADJRES'+i]
#resct=resct[resloclist]
#resct.to_csv(path+'nyctract/resct2.csv',index=True,na_rep='999')


#
#
#
## NYC Work Censust Blocks
#adjwork=pd.DataFrame()
#for i in adjlist:
#    tp=pd.read_csv(path+'nyctract/work/ADJWORK'+i+'.csv',dtype=str)
#    tp=tp.set_index('blockid')
#    adjwork=pd.concat([adjwork,tp],axis=1)
#workbk=pd.read_csv(path+'nyctract/workbk.csv',dtype=str)
#workbk=workbk.set_index('blockid')
#workloclist=workbk.columns
#workbk=pd.concat([workbk,adjwork],axis=1)
#for i in adjlist:
#    workbk['WORK'+i]=workbk['ADJWORK'+i]
#workbk=workbk[workloclist]
#workbk.to_csv(path+'nyctract/workbk2.csv',index=True)
## NYC Work Censust Tracts
#adjworkloclist=adjwork.columns
#for i in adjwork.columns:
#    adjwork[i]=pd.to_numeric(adjwork[i])
#adjwork=adjwork.replace(999,np.nan)
#adjwork['tractid']=[str(x)[0:11] for x in adjwork.index]
#adjwork=adjwork.groupby(['tractid'])[adjworkloclist].median(skipna=True)
#workct=pd.read_csv(path+'nyctract/workct.csv',dtype=str)
#workct=workct.set_index('tractid')
#workloclist=workct.columns
#for i in workct.columns:
#    workct[i]=pd.to_numeric(workct[i])
#workct=pd.concat([workct,adjwork],axis=1)
#for i in adjlist:
#    workct['WORK'+i]=workct['ADJWORK'+i]
#workct=workct[workloclist]
#workct.to_csv(path+'nyctract/workct2.csv',index=True,na_rep='999')
#
#
#
#
#
#
#
#
#
#
#
#
#
#
## Map checks
#
## Define add basemap
#def add_basemap(ax, zoom, url='http://tile.stamen.com/terrain/tileZ/tileX/tileY.png'):
#    xmin, xmax, ymin, ymax = ax.axis()
#    basemap, extent = ctx.bounds2img(xmin, ymin, xmax, ymax, zoom=zoom, url=url)
#    ax.imshow(basemap, extent=extent, interpolation='bilinear')
#    ax.axis((xmin, xmax, ymin, ymax))
#
## Map adjusted walk and transit
#ct=gpd.read_file(path+'shp/quadstatectclipped.shp')
#ct.crs={'init': 'epsg:4326'}
#ct=ct[['tractid','geometry']]
## Res
#resct=pd.read_csv(path+'nyctract/resct2.csv',dtype=str)
#for i in resct.columns[1:]:
#    resct[i]=pd.to_numeric(resct[i])
#resct=ct.merge(resct,on='tractid')
#for i in ['RES'+x for x in adjlist]:
#    resctmap=resct.loc[resct[i]<=120,[i,'geometry']]
#    resctmap=resctmap.to_crs(epsg=3857)
#    fig,ax=plt.subplots(1,figsize=(11,8.5))
#    plt.subplots_adjust(left=0.05,right=0.95,top=0.95,bottom=0.05)
#    ax=resctmap.plot(figsize=(11,8.5),edgecolor=None,column=i,cmap='Spectral',linewidth=0.2,ax=ax,alpha=0.7)
#    add_basemap(ax,zoom=11,url=ctx.sources.ST_TONER_LITE)
#    ax.set_axis_off()
#    sm=plt.cm.ScalarMappable(cmap='Spectral',norm=plt.Normalize(vmin=1,vmax=90))
#    sm._A=[]
#    divider=mpl_toolkits.axes_grid1.make_axes_locatable(ax)
#    cax=divider.append_axes("right",size="3%",pad=0.2,aspect=25)
#    cbar=fig.colorbar(sm,cax=cax)
#    fig.tight_layout()
#    fig.savefig(path+'nyctract/res/ADJRES'+i+'ct.jpg', dpi=300)
## Work
#workct=pd.read_csv(path+'nyctract/workct2.csv',dtype=str)
#for i in workct.columns[1:]:
#    workct[i]=pd.to_numeric(workct[i])
#workct=ct.merge(workct,on='tractid')
#for i in ['WORK'+x for x in adjlist]:
#    workctmap=workct.loc[workct[i]<=120,[i,'geometry']]
#    workctmap=workctmap.to_crs(epsg=3857)
#    fig,ax=plt.subplots(1,figsize=(11,8.5))
#    plt.subplots_adjust(left=0.05,right=0.95,top=0.95,bottom=0.05)
#    ax=workctmap.plot(figsize=(11,8.5),edgecolor=None,column=i,cmap='Spectral',linewidth=0.2,ax=ax,alpha=0.7)
#    add_basemap(ax,zoom=11,url=ctx.sources.ST_TONER_LITE)
#    ax.set_axis_off()
#    sm=plt.cm.ScalarMappable(cmap='Spectral',norm=plt.Normalize(vmin=1,vmax=90))
#    sm._A=[]
#    divider=mpl_toolkits.axes_grid1.make_axes_locatable(ax)
#    cax=divider.append_axes("right",size="3%",pad=0.2,aspect=25)
#    cbar=fig.colorbar(sm,cax=cax)
#    fig.tight_layout()
#    fig.savefig(path+'nyctract/work/ADJWORK'+i+'ct.jpg', dpi=300)
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
## Block Level Gravity Model
## Res Gravity
#adjresbkwac=pd.DataFrame()
#for i in adjlist:
#    tp=pd.read_csv(path+'nyctract/res/ADJRES'+i+'.csv',dtype=str)
#    tp=tp.set_index('blockid')
#    adjresbkwac=pd.concat([adjresbkwac,tp],axis=1)
#adjresloclist=adjresbkwac.columns
#wac=pd.DataFrame()
#for i in ['ct','nj','ny','pa']:
#    tp=pd.read_csv(path+'lehd/'+str(i)+'_wac_S000_JT03_2015.csv',dtype=str)
#    tp=tp[['w_geocode','C000']]
#    wac=pd.concat([wac,tp],axis=0)
#wac.columns=['blockid','wac']
#wac=wac.set_index('blockid')
#adjresbkwac=pd.merge(adjresbkwac,wac,how='left',left_index=True,right_index=True)
#adjresbkwac['wac']=adjresbkwac['wac'].replace(np.nan,'0')
#for i in adjresbkwac.columns:
#    adjresbkwac[i]=pd.to_numeric(adjresbkwac[i])
#for i in adjresloclist:
#    adjresbkwac[i]=np.where(adjresbkwac[i]<=10,5,
#                   np.where(adjresbkwac[i]<=20,15,
#                   np.where(adjresbkwac[i]<=30,25,
#                   np.where(adjresbkwac[i]<=40,35,
#                   np.where(adjresbkwac[i]<=50,45,
#                   np.where(adjresbkwac[i]<=60,55,
#                   np.nan))))))
#adjresbkgravity=pd.DataFrame(index=adjresloclist,columns=['WAC1-10','WAC11-20','WAC21-30','WAC31-40','WAC41-50','WAC51-60',
#                                                          'GWAC1-10','GWAC11-20','GWAC21-30','GWAC31-40','GWAC41-50','GWAC51-60',
#                                                          'GRAVITYWAC'])
#for i in adjresloclist:
#    tp=sum(adjresbkwac.loc[adjresbkwac[i]==5,'wac'])
#    adjresbkgravity.loc[i,'WAC1-10']=tp
#    tp=sum(adjresbkwac.loc[adjresbkwac[i]==15,'wac'])
#    adjresbkgravity.loc[i,'WAC11-20']=tp
#    tp=sum(adjresbkwac.loc[adjresbkwac[i]==25,'wac'])
#    adjresbkgravity.loc[i,'WAC21-30']=tp
#    tp=sum(adjresbkwac.loc[adjresbkwac[i]==35,'wac'])
#    adjresbkgravity.loc[i,'WAC31-40']=tp
#    tp=sum(adjresbkwac.loc[adjresbkwac[i]==45,'wac'])
#    adjresbkgravity.loc[i,'WAC41-50']=tp
#    tp=sum(adjresbkwac.loc[adjresbkwac[i]==55,'wac'])
#    adjresbkgravity.loc[i,'WAC51-60']=tp
#    adjresbkgravity.loc[i,'GWAC1-10']=(adjresbkgravity.loc[i,'WAC1-10'])/(5**2)
#    adjresbkgravity.loc[i,'GWAC11-20']=(adjresbkgravity.loc[i,'WAC11-20'])/(15**2)
#    adjresbkgravity.loc[i,'GWAC21-30']=(adjresbkgravity.loc[i,'WAC21-30'])/(25**2)
#    adjresbkgravity.loc[i,'GWAC31-40']=(adjresbkgravity.loc[i,'WAC31-40'])/(35**2)
#    adjresbkgravity.loc[i,'GWAC41-50']=(adjresbkgravity.loc[i,'WAC41-50'])/(45**2)
#    adjresbkgravity.loc[i,'GWAC51-60']=(adjresbkgravity.loc[i,'WAC51-60'])/(55**2)
#    adjresbkgravity.loc[i,'GRAVITYWAC']=adjresbkgravity.loc[i,'GWAC1-10']+adjresbkgravity.loc[i,'GWAC11-20']+adjresbkgravity.loc[i,'GWAC21-30']+adjresbkgravity.loc[i,'GWAC31-40']+adjresbkgravity.loc[i,'GWAC41-50']+adjresbkgravity.loc[i,'GWAC51-60']
#resbkgravity=pd.read_csv(path+'nyctract/resbkgravity.csv',dtype=str)
#resbkgravity=resbkgravity.set_index('Unnamed: 0')
#for i in resbkgravity.columns:
#    resbkgravity[i]=pd.to_numeric(resbkgravity[i])
#for i in adjlist:
#    resbkgravity.loc['RES'+i,:]=adjresbkgravity.loc['ADJRES'+i,:]
#resbkgravity.to_csv(path+'nyctract/resbkgravity2.csv',index=True)
#
#
#
#
#
## Work Gravity
#adjworkbkrac=pd.DataFrame()
#for i in adjlist:
#    tp=pd.read_csv(path+'nyctract/work/ADJWORK'+i+'.csv',dtype=str)
#    tp=tp.set_index('blockid')
#    adjworkbkrac=pd.concat([adjworkbkrac,tp],axis=1)
#adjworkloclist=adjworkbkrac.columns
#rac=pd.DataFrame()
#for i in ['ct','nj','ny','pa']:
#    tp=pd.read_csv(path+'lehd/'+str(i)+'_rac_S000_JT03_2015.csv',dtype=str)
#    tp=tp[['h_geocode','C000']]
#    rac=pd.concat([rac,tp],axis=0)
#rac.columns=['blockid','rac']
#rac=rac.set_index('blockid')
#adjworkbkrac=pd.merge(adjworkbkrac,rac,how='left',left_index=True,right_index=True)
#adjworkbkrac['rac']=adjworkbkrac['rac'].replace(np.nan,'0')
#for i in adjworkbkrac.columns:
#    adjworkbkrac[i]=pd.to_numeric(adjworkbkrac[i])
#for i in adjworkloclist:
#    adjworkbkrac[i]=np.where(adjworkbkrac[i]<=10,5,
#                    np.where(adjworkbkrac[i]<=20,15,
#                    np.where(adjworkbkrac[i]<=30,25,
#                    np.where(adjworkbkrac[i]<=40,35,
#                    np.where(adjworkbkrac[i]<=50,45,
#                    np.where(adjworkbkrac[i]<=60,55,
#                    np.nan))))))
#adjworkbkgravity=pd.DataFrame(index=adjworkloclist,columns=['RAC1-10','RAC11-20','RAC21-30','RAC31-40','RAC41-50','RAC51-60',
#                                                            'GRAC1-10','GRAC11-20','GRAC21-30','GRAC31-40','GRAC41-50','GRAC51-60',
#                                                            'GRAVITYRAC'])
#for i in adjworkloclist:
#    tp=sum(adjworkbkrac.loc[adjworkbkrac[i]==5,'rac'])
#    adjworkbkgravity.loc[i,'RAC1-10']=tp
#    tp=sum(adjworkbkrac.loc[adjworkbkrac[i]==15,'rac'])
#    adjworkbkgravity.loc[i,'RAC11-20']=tp
#    tp=sum(adjworkbkrac.loc[adjworkbkrac[i]==25,'rac'])
#    adjworkbkgravity.loc[i,'RAC21-30']=tp
#    tp=sum(adjworkbkrac.loc[adjworkbkrac[i]==35,'rac'])
#    adjworkbkgravity.loc[i,'RAC31-40']=tp
#    tp=sum(adjworkbkrac.loc[adjworkbkrac[i]==45,'rac'])
#    adjworkbkgravity.loc[i,'RAC41-50']=tp
#    tp=sum(adjworkbkrac.loc[adjworkbkrac[i]==55,'rac'])
#    adjworkbkgravity.loc[i,'RAC51-60']=tp
#    adjworkbkgravity.loc[i,'GRAC1-10']=(adjworkbkgravity.loc[i,'RAC1-10'])/(5**2)
#    adjworkbkgravity.loc[i,'GRAC11-20']=(adjworkbkgravity.loc[i,'RAC11-20'])/(15**2)
#    adjworkbkgravity.loc[i,'GRAC21-30']=(adjworkbkgravity.loc[i,'RAC21-30'])/(25**2)
#    adjworkbkgravity.loc[i,'GRAC31-40']=(adjworkbkgravity.loc[i,'RAC31-40'])/(35**2)
#    adjworkbkgravity.loc[i,'GRAC41-50']=(adjworkbkgravity.loc[i,'RAC41-50'])/(45**2)
#    adjworkbkgravity.loc[i,'GRAC51-60']=(adjworkbkgravity.loc[i,'RAC51-60'])/(55**2)
#    adjworkbkgravity.loc[i,'GRAVITYRAC']=adjworkbkgravity.loc[i,'GRAC1-10']+adjworkbkgravity.loc[i,'GRAC11-20']+adjworkbkgravity.loc[i,'GRAC21-30']+adjworkbkgravity.loc[i,'GRAC31-40']+adjworkbkgravity.loc[i,'GRAC41-50']+adjworkbkgravity.loc[i,'GRAC51-60']
#workbkgravity=pd.read_csv(path+'nyctract/workbkgravity.csv',dtype=str)
#workbkgravity=workbkgravity.set_index('Unnamed: 0')
#for i in workbkgravity.columns:
#    workbkgravity[i]=pd.to_numeric(workbkgravity[i])
#for i in adjlist:
#    workbkgravity.loc['WORK'+i,:]=adjworkbkgravity.loc['ADJWORK'+i,:]
#workbkgravity.to_csv(path+'nyctract/workbkgravity2.csv',index=True)
#
#
#
#
#
#
#
### Tract Level Gravity Model
## Res Gravity
#resctwac=pd.read_csv(path+'nyctract/resct2.csv',dtype=str)
#resctwac=resctwac.set_index('tractid')
#resloclist=sorted(resctwac.columns)
#wac=pd.DataFrame()
#for i in ['ct','nj','ny','pa']:
#    tp=pd.read_csv(path+'lehd/'+str(i)+'_wac_S000_JT03_2015.csv',dtype=str)
#    tp=tp[['w_geocode','C000']]
#    wac=pd.concat([wac,tp],axis=0)
#wac.columns=['blockid','wac']
#wac['tractid']=[str(x)[0:11] for x in wac['blockid']]
#wac['wac']=pd.to_numeric(wac['wac'])
#wac=pd.DataFrame(wac.groupby('tractid')['wac'].sum())
#resctwac=pd.merge(resctwac,wac,how='left',left_index=True,right_index=True)
#resctwac['wac']=resctwac['wac'].replace(np.nan,'0')
#for i in resctwac.columns:
#    resctwac[i]=pd.to_numeric(resctwac[i])
#for i in resloclist:
#    resctwac[i]=np.where(resctwac[i]<=10,5,
#                np.where(resctwac[i]<=20,15,
#                np.where(resctwac[i]<=30,25,
#                np.where(resctwac[i]<=40,35,
#                np.where(resctwac[i]<=50,45,
#                np.where(resctwac[i]<=60,55,
#                np.nan))))))
#resctgravity=pd.DataFrame(index=resloclist,columns=['WAC1-10','WAC11-20','WAC21-30','WAC31-40','WAC41-50','WAC51-60',
#                                                    'GWAC1-10','GWAC11-20','GWAC21-30','GWAC31-40','GWAC41-50','GWAC51-60',
#                                                    'GRAVITYWAC'])
#for i in resloclist:
#    tp=sum(resctwac.loc[resctwac[i]==5,'wac'])
#    resctgravity.loc[i,'WAC1-10']=tp
#    tp=sum(resctwac.loc[resctwac[i]==15,'wac'])
#    resctgravity.loc[i,'WAC11-20']=tp
#    tp=sum(resctwac.loc[resctwac[i]==25,'wac'])
#    resctgravity.loc[i,'WAC21-30']=tp
#    tp=sum(resctwac.loc[resctwac[i]==35,'wac'])
#    resctgravity.loc[i,'WAC31-40']=tp
#    tp=sum(resctwac.loc[resctwac[i]==45,'wac'])
#    resctgravity.loc[i,'WAC41-50']=tp
#    tp=sum(resctwac.loc[resctwac[i]==55,'wac'])
#    resctgravity.loc[i,'WAC51-60']=tp
#    resctgravity.loc[i,'GWAC1-10']=(resctgravity.loc[i,'WAC1-10'])/(5**2)
#    resctgravity.loc[i,'GWAC11-20']=(resctgravity.loc[i,'WAC11-20'])/(15**2)
#    resctgravity.loc[i,'GWAC21-30']=(resctgravity.loc[i,'WAC21-30'])/(25**2)
#    resctgravity.loc[i,'GWAC31-40']=(resctgravity.loc[i,'WAC31-40'])/(35**2)
#    resctgravity.loc[i,'GWAC41-50']=(resctgravity.loc[i,'WAC41-50'])/(45**2)
#    resctgravity.loc[i,'GWAC51-60']=(resctgravity.loc[i,'WAC51-60'])/(55**2)
#    resctgravity.loc[i,'GRAVITYWAC']=resctgravity.loc[i,'GWAC1-10']+resctgravity.loc[i,'GWAC11-20']+resctgravity.loc[i,'GWAC21-30']+resctgravity.loc[i,'GWAC31-40']+resctgravity.loc[i,'GWAC41-50']+resctgravity.loc[i,'GWAC51-60']
#resctgravity.to_csv(path+'nyctract/resctgravity2.csv',index=True)
#
## Work Gravity
#workctrac=pd.read_csv(path+'nyctract/workct2.csv',dtype=str)
#workctrac=workctrac.set_index('tractid')
#workloclist=sorted(workctrac.columns)
#rac=pd.DataFrame()
#for i in ['ct','nj','ny','pa']:
#    tp=pd.read_csv(path+'lehd/'+str(i)+'_rac_S000_JT03_2015.csv',dtype=str)
#    tp=tp[['h_geocode','C000']]
#    rac=pd.concat([rac,tp],axis=0)
#rac.columns=['blockid','rac']
#rac['tractid']=[str(x)[0:11] for x in rac['blockid']]
#rac['rac']=pd.to_numeric(rac['rac'])
#rac=pd.DataFrame(rac.groupby('tractid')['rac'].sum())
#workctrac=pd.merge(workctrac,rac,how='left',left_index=True,right_index=True)
#workctrac['rac']=workctrac['rac'].replace(np.nan,'0')
#for i in workctrac.columns:
#    workctrac[i]=pd.to_numeric(workctrac[i])
#for i in workloclist:
#    workctrac[i]=np.where(workctrac[i]<=10,5,
#                 np.where(workctrac[i]<=20,15,
#                 np.where(workctrac[i]<=30,25,
#                 np.where(workctrac[i]<=40,35,
#                 np.where(workctrac[i]<=50,45,
#                 np.where(workctrac[i]<=60,55,
#                 np.nan))))))
#workctgravity=pd.DataFrame(index=workloclist,columns=['RAC1-10','RAC11-20','RAC21-30','RAC31-40','RAC41-50','RAC51-60',
#                                                      'GRAC1-10','GRAC11-20','GRAC21-30','GRAC31-40','GRAC41-50','GRAC51-60',
#                                                      'GRAVITYRAC'])
#for i in workloclist:
#    tp=sum(workctrac.loc[workctrac[i]==5,'rac'])
#    workctgravity.loc[i,'RAC1-10']=tp
#    tp=sum(workctrac.loc[workctrac[i]==15,'rac'])
#    workctgravity.loc[i,'RAC11-20']=tp
#    tp=sum(workctrac.loc[workctrac[i]==25,'rac'])
#    workctgravity.loc[i,'RAC21-30']=tp
#    tp=sum(workctrac.loc[workctrac[i]==35,'rac'])
#    workctgravity.loc[i,'RAC31-40']=tp
#    tp=sum(workctrac.loc[workctrac[i]==45,'rac'])
#    workctgravity.loc[i,'RAC41-50']=tp
#    tp=sum(workctrac.loc[workctrac[i]==55,'rac'])
#    workctgravity.loc[i,'RAC51-60']=tp
#    workctgravity.loc[i,'GRAC1-10']=(workctgravity.loc[i,'RAC1-10'])/(5**2)
#    workctgravity.loc[i,'GRAC11-20']=(workctgravity.loc[i,'RAC11-20'])/(15**2)
#    workctgravity.loc[i,'GRAC21-30']=(workctgravity.loc[i,'RAC21-30'])/(25**2)
#    workctgravity.loc[i,'GRAC31-40']=(workctgravity.loc[i,'RAC31-40'])/(35**2)
#    workctgravity.loc[i,'GRAC41-50']=(workctgravity.loc[i,'RAC41-50'])/(45**2)
#    workctgravity.loc[i,'GRAC51-60']=(workctgravity.loc[i,'RAC51-60'])/(55**2)
#    workctgravity.loc[i,'GRAVITYRAC']=workctgravity.loc[i,'GRAC1-10']+workctgravity.loc[i,'GRAC11-20']+workctgravity.loc[i,'GRAC21-30']+workctgravity.loc[i,'GRAC31-40']+workctgravity.loc[i,'GRAC41-50']+workctgravity.loc[i,'GRAC51-60']
#workctgravity.to_csv(path+'nyctract/workctgravity2.csv',index=True)
#












