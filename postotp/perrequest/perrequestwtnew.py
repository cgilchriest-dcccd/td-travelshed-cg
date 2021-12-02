#! /usr/bin/python3
# Travelshed Mapping
# OpenTripPlanner Guide: http://docs.opentripplanner.org/en/latest/
# OpenTripPlanner API Doc: http://dev.opentripplanner.org/apidoc/1.0.0/index.html

import datetime
import time
import geopandas as gpd
import pandas as pd
import numpy as np
import shapely
import requests
import multiprocessing as mp
import plotly.graph_objects as go
import plotly.io as pio
import json



pd.set_option('display.max_columns', None)
pio.renderers.default='browser'
# path='/home/mayijun/TRAVELSHED/'
path='C:/Users/mayij/Desktop/DOC/DCP2018/TRAVELSHEDREVAMP/'
doserver='http://159.65.64.166:8801/'
# doserver='http://localhost:8801/'



start=datetime.datetime.now()

# Site location
site=pd.read_excel(path+'waterfront2/input.xlsx',sheet_name='input',dtype=str)
site=gpd.GeoDataFrame(site,geometry=[shapely.geometry.Point(xy) for xy in zip(pd.to_numeric(site['long']), pd.to_numeric(site['lat']))],crs=4326)
sitegeom=site['geometry']

# Nearest intersection
node=gpd.read_file(path+'location/node.shp')
node.crs=4326
node=node['geometry']
node=shapely.geometry.MultiPoint(node)
nr=[shapely.ops.nearest_points(x,node)[1] for x in sitegeom]
for i in site.index:
    site.loc[i,'intlat']=nr[i].y
    site.loc[i,'intlong']=nr[i].x
site=site[['siteid','parkid','direction','name','type','borough','lat','long','intlat','intlong','distance',
            'walktime']].reset_index(drop=True)

# Distance from site to nearest intersection
frompt=site.copy()
frompt=gpd.GeoDataFrame(frompt,geometry=[shapely.geometry.Point(xy) for xy in zip(pd.to_numeric(frompt['long']), pd.to_numeric(frompt['lat']))],crs=4326)
frompt=frompt.to_crs(6539)
frompt=frompt['geometry']
topt=site.copy()
topt=gpd.GeoDataFrame(topt,geometry=[shapely.geometry.Point(xy) for xy in zip(pd.to_numeric(topt['intlong']), pd.to_numeric(topt['intlat']))],crs=4326)
topt=topt.to_crs(6539)
topt=topt['geometry']
dist=frompt.distance(topt)
site['distance']=dist

# Walk time from site to nearest intersection
for i in site.index:
    url=doserver+'otp/routers/default/plan?fromPlace='
    url+=str(site.loc[i,'lat'])+','+str(site.loc[i,'long'])
    url+='&toPlace='+str(site.loc[i,'intlat'])+','+str(site.loc[i,'intlong'])+'&mode=WALK'
    headers={'Accept':'application/json'}
    req=requests.get(url=url,headers=headers)
    js=req.json()
    if list(js.keys())[1]=='error':
        site.loc[i,'walktime']=np.nan
    else:
        site.loc[i,'walktime']=js['plan']['itineraries'][0]['legs'][0]['duration']
    time.sleep(0.1)
site.to_excel(path+'waterfront2/input.xlsx',sheet_name='input',index=False)



# Load quadstate block point shapefile
bkpt=gpd.read_file(path+'shp/quadstatebkpt.shp')
bkpt.crs=4326

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
maxWalkDistance=805 # in meters

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
def travelshedwt(arrt):
    bk=bkpt.copy()
    if destination.loc[i,'direction']=='to':
        url=doserver+'otp/routers/default/isochrone?batch=true&mode=WALK,TRANSIT'
        url+='&fromPlace='+destination.loc[i,'latlong']+'&toPlace='+destination.loc[i,'latlong']
        url+='&arriveBy=true&date='+typicaldate+'&time='+arrt+'&maxTransfers='+str(maxTransfers)
        url+='&maxWalkDistance='+str(maxWalkDistance)+'&clampInitialWait=-1'+cutoff
    elif destination.loc[i,'direction']=='from':
        url=doserver+'otp/routers/default/isochrone?batch=true&mode=WALK,TRANSIT'
        url+='&fromPlace='+destination.loc[i,'latlong']
        url+='&date='+typicaldate+'&time='+arrt+'&maxTransfers='+str(maxTransfers)
        url+='&maxWalkDistance='+str(maxWalkDistance)+'&clampInitialWait=0'+cutoff
    else:
        print(destination.loc[i,'id']+' has no direction!')
    headers={'Accept':'application/json'}
    req=requests.get(url=url,headers=headers)
    js=req.json()
    iso=gpd.GeoDataFrame.from_features(js,crs=4326)
    bk['T'+arrt[0:2]+arrt[3:5]]=999
    cut=range(cutoffend,cutoffstart,-cutoffinterval)
    if iso.loc[iso['time']==cut[0]*60,'geometry'].notna().bool():
        try:
            bkiso=gpd.sjoin(bk,iso.loc[iso['time']==cut[0]*60],how='inner',op='within')
            bkiso=bkiso['blockid']
            bk.loc[bk['blockid'].isin(bkiso),'T'+arrt[0:2]+arrt[3:5]]=cut[0]-cutoffinterval/2
        except:
            print(destination.loc[i,'id']+' '+arrt+' '+str(cut[0])+'-minute isochrone has no Census Block in it!')
        for k in range(0,(len(cut)-1)):
            if iso.loc[iso['time']==cut[k+1]*60,'geometry'].notna().bool():
                if len(bk.loc[bk['T'+arrt[0:2]+arrt[3:5]]==cut[k]-cutoffinterval/2])!=0:
                    try:
                        bkiso=gpd.sjoin(bk.loc[bk['T'+arrt[0:2]+arrt[3:5]]==cut[k]-cutoffinterval/2],
                                        iso.loc[iso['time']==cut[k+1]*60],how='inner',op='within')
                        bkiso=bkiso['blockid']
                        bk.loc[bk['blockid'].isin(bkiso),'T'+arrt[0:2]+arrt[3:5]]=cut[k+1]-cutoffinterval/2
                    except ValueError:
                        print(destination.loc[i,'id']+' '+arrt+' '+str(cut[k+1])+'-minute isochrone has no Census Block in it!')
                else:
                    print(destination.loc[i,'id']+' '+arrt+' '+str(cut[k])+'-minute isochrone has no Census Block in it!')
            else:
                print(destination.loc[i,'id']+' '+arrt+' '+str(cut[k+1])+'-minute isochrone has no geometry!')
    else:
        print(destination.loc[i,'id']+' '+arrt+' '+str(cut[0])+'-minute isochrone has no geometry!')
    bk['T'+arrt[0:2]+arrt[3:5]]=bk['T'+arrt[0:2]+arrt[3:5]].replace(999,np.nan)
    bk=bk.drop(['geometry'],axis=1)
    bk=bk.set_index('blockid')
    return bk



## Define parallel multiprocessing function
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



# Multiprocessing travelshed function for sites
if __name__=='__main__':
    location=pd.read_excel(path+'waterfront2/input.xlsx',sheet_name='input',dtype=str)
    location['id']=['SITE'+str(x).zfill(4) for x in location['siteid']]
    location['latlong']=[str(x)+','+str(y) for x,y in zip(location['lat'],location['long'])]
    destination=location.loc[0:max(location.count())-1,['id','direction','latlong']].reset_index(drop=True)
    # Create travel time table for each site
    for i in destination.index:
        df=parallelize(arrivaltime,travelshedwt)
        df['TTMEDIAN']=df.median(skipna=True,axis=1)
        df=df['TTMEDIAN'].sort_index()
        df.name=destination.loc[i,'id']
        df.to_csv(path+'perrequest/'+destination.loc[i,'id']+'wt.csv',index=True,header=True,na_rep=999)
    
    # Join site travelsheds to block shapefile
    wtbk=gpd.read_file(path+'shp/quadstatebkclipped.shp')
    wtbk.crs=4326
    wtbk=wtbk.drop(['lat','long'],axis=1)
    for i in destination.index:
        tp=pd.read_csv(path+'perrequest/'+destination.loc[i,'id']+'wt.csv',dtype=float,converters={'blockid':str})
        wtbk=wtbk.merge(tp,on='blockid')
    wtbk.to_file(path+'perrequest/wtbk.shp')
    wtbk=wtbk.drop('geometry',axis=1)
    wtbk.to_csv(path+'perrequest/wtbk.csv',index=False)
    # Join site travelsheds to tract shapefile
    wtbk=wtbk.replace(999,np.nan)
    loclist=wtbk.columns[1:]
    wtbk['tractid']=[str(x)[0:11] for x in wtbk['blockid']]
    wtbk=wtbk.groupby(['tractid'],as_index=False)[loclist].median()
    wtbk=wtbk.replace(np.nan,999)
    wtct=gpd.read_file(path+'shp/quadstatectclipped.shp')
    wtct.crs=4326
    wtct=wtct.drop(['lat','long'],axis=1)
    wtct=wtct.merge(wtbk,on='tractid')
    wtct.to_file(path+'perrequest/wtct.shp')
    wtct=wtct.drop('geometry',axis=1)
    wtct.to_csv(path+'perrequest/wtct.csv',index=False)
    # Create map for each site
    wtbk=gpd.read_file(path+'perrequest/wtbk.shp')
    wtbk.crs=4326
    wtct=gpd.read_file(path+'perrequest/wtct.shp')
    wtct.crs=4326
    for i in destination.index:
        # Create block level map
        wtbkmap=wtbk.loc[wtbk[destination.loc[i,'id']]<=120,['blockid',destination.loc[i,'id'],'geometry']].reset_index(drop=True)
        wtbkgjs=json.loads(wtbkmap.to_json())
        p=go.Figure(go.Choroplethmapbox(geojson=wtbkgjs,
                                        featureidkey='properties.blockid',
                                        locations=wtbkmap['blockid'],
                                        z=wtbkmap[destination.loc[i,'id']],
                                        zmin=0,
                                        zmax=120,
                                        colorscale='RdBu',
                                        reversescale=False,
                                        colorbar={'title_text':'<b>Travel<br>Time<br>(mins)<br> <br></b>',
                                                  'title_font_size':16,
                                                  'x':0.99,
                                                  'xanchor':'right',
                                                  'xpad':20,
                                                  'y':0.99,
                                                  'yanchor':'top',
                                                  'ypad':20,
                                                  'len':0.5,
                                                  'outlinewidth':0,
                                                  'tickvals':list(range(0,100,10)),
                                                  'tickfont_size':12,
                                                  'bgcolor':'rgba(255,255,255,0.5)'},
                                        marker={'line_color':'rgba(255,255,255,0)',
                                                'line_width':0,
                                                'opacity':0.8},
                                    hovertext='<b>Census Tract: </b>'+
                                              wtbkmap['blockid']+
                                              '<br>'+
                                              '<b>Transit Travel Time (mins): </b>'+
                                              wtbkmap[destination.loc[i,'id']].astype(int).astype(str),
                                    hoverinfo='text'))
        p=p.add_trace(go.Scattermapbox(lat=[pd.to_numeric(destination.loc[i,'latlong'].split(',')[0])],
                        lon=[pd.to_numeric(destination.loc[i,'latlong'].split(',')[-1])],
                        mode='markers',
                        marker={'size':5,
                                'color':'black'},
                        hoverinfo='none'))
        p.update_layout(mapbox={'style':'carto-positron',
                                'center':{'lat':np.mean([min(wtbkmap.bounds['miny']),max(wtbkmap.bounds['maxy'])]),
                                        'lon':np.mean([min(wtbkmap.bounds['minx']),max(wtbkmap.bounds['maxx'])])},
                                'zoom':8.5},
                        title={'text':'<b>Transit Travel Time '+destination.loc[i,'direction']+' '+destination.loc[i,'id']+'</b>',
                                'font_size':20},
                        template='ggplot2',
                        font={'family':'arial',
                              'color':'black'},
                        margin={'r':0,'t':40,'l':0,'b':0})
        p.write_image(path+'perrequest/'+destination.loc[i,'id']+'wtbk.jpeg',width=1600,height=900)
        p.write_html(path+'perrequest/'+destination.loc[i,'id']+'wtbk.html',
                      include_plotlyjs='cdn',
                      config={'displaylogo':False,'modeBarButtonsToRemove':['select2d','lasso2d']})
        # Create tract level map
        wtctmap=wtct.loc[wtct[destination.loc[i,'id']]<=120,['tractid',destination.loc[i,'id'],'geometry']].reset_index(drop=True)
        wtctgjs=json.loads(wtctmap.to_json())
        p=go.Figure(go.Choroplethmapbox(geojson=wtctgjs,
                                        featureidkey='properties.tractid',
                                        locations=wtctmap['tractid'],
                                        z=wtctmap[destination.loc[i,'id']],
                                        zmin=0,
                                        zmax=120,
                                        colorscale='RdBu',
                                        reversescale=False,
                                        colorbar={'title_text':'<b>Travel<br>Time<br>(mins)<br> <br></b>',
                                                  'title_font_size':16,
                                                  'x':0.99,
                                                  'xanchor':'right',
                                                  'xpad':20,
                                                  'y':0.99,
                                                  'yanchor':'top',
                                                  'ypad':20,
                                                  'len':0.5,
                                                  'outlinewidth':0,
                                                  'tickvals':list(range(0,100,10)),
                                                  'tickfont_size':12,
                                                  'bgcolor':'rgba(255,255,255,0.5)'},
                                        marker={'line_color':'rgba(255,255,255,0)',
                                                'line_width':0,
                                                'opacity':0.8},
                                    hovertext='<b>Census Tract: </b>'+
                                              wtctmap['tractid']+
                                              '<br>'+
                                              '<b>Transit Travel Time (mins): </b>'+
                                              wtctmap[destination.loc[i,'id']].astype(int).astype(str),
                                    hoverinfo='text'))
        p=p.add_trace(go.Scattermapbox(lat=[pd.to_numeric(destination.loc[i,'latlong'].split(',')[0])],
                        lon=[pd.to_numeric(destination.loc[i,'latlong'].split(',')[-1])],
                        mode='markers',
                        marker={'size':5,
                                'color':'black'},
                        hoverinfo='none'))
        p.update_layout(mapbox={'style':'carto-positron',
                                'center':{'lat':np.mean([min(wtctmap.bounds['miny']),max(wtctmap.bounds['maxy'])]),
                                        'lon':np.mean([min(wtctmap.bounds['minx']),max(wtctmap.bounds['maxx'])])},
                                'zoom':8.5},
                        title={'text':'<b>Transit Travel Time '+destination.loc[i,'direction']+' '+destination.loc[i,'id']+'</b>',
                                'font_size':20},
                        template='ggplot2',
                        font={'family':'arial',
                              'color':'black'},
                        margin={'r':0,'t':40,'l':0,'b':0})
        p.write_image(path+'perrequest/'+destination.loc[i,'id']+'wtct.jpeg',width=1600,height=900)
        p.write_html(path+'perrequest/'+destination.loc[i,'id']+'wtct.html',
                      include_plotlyjs='cdn',
                      config={'displaylogo':False,'modeBarButtonsToRemove':['select2d','lasso2d']})
    print(datetime.datetime.now()-start)

