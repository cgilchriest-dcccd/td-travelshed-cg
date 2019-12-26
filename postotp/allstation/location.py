import pandas as pd
import geopandas as gpd
import shapely
import numpy as np
import requests
import time


path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2018/TRAVELSHEDREVAMP/location/'


# Subway
# Intersection points
node=gpd.read_file(path+'node.shp')
node=node['geometry']
node=shapely.geometry.MultiPoint(node)

# Subway points
subway=pd.read_excel(path+'location.xlsx',sheet_name='subway')
subway=gpd.GeoDataFrame(subway,crs={'init': 'epsg:4326'},
                        geometry=[shapely.geometry.Point(xy) for xy in zip(subway['long'], subway['lat'])])
subway=subway['geometry']

# Nearest intersections
nr=[shapely.ops.nearest_points(x,node)[1] for x in subway]
subway=pd.read_excel(path+'location.xlsx',sheet_name='subway')
subway=gpd.GeoDataFrame(subway,crs={'init': 'epsg:4326'},
                        geometry=[shapely.geometry.Point(xy) for xy in zip(subway['long'], subway['lat'])])
subway['intlat']=np.nan
subway['intlong']=np.nan
for i in subway.index:
    subway.loc[i,'intlat']=nr[i].y
    subway.loc[i,'intlong']=nr[i].x
subway=subway[['subwayid','boro','name','routes','lat','long','intlat','intlong']]
subway.to_csv(path+'subway.csv',index=False)

# Distance from subway points to nearest intersections
frompt=subway.copy()
frompt=gpd.GeoDataFrame(frompt,crs={'init': 'epsg:4326'},
                        geometry=[shapely.geometry.Point(xy) for xy in zip(frompt['long'], frompt['lat'])])
frompt=frompt.to_crs({'init': 'epsg:6539'})
frompt=frompt['geometry']
topt=subway.copy()
topt=gpd.GeoDataFrame(topt,crs={'init': 'epsg:4326'},
                      geometry=[shapely.geometry.Point(xy) for xy in zip(topt['intlong'], topt['intlat'])])
topt=topt.to_crs({'init': 'epsg:6539'})
topt=topt['geometry']
dist=frompt.distance(topt)
subway['distance']=dist
subway.to_csv(path+'subway.csv',index=False)

# Walk time from subway points to nearest intersections
subway['time']=np.nan
for i in subway.index:
    url='http://localhost:8801/otp/routers/default/plan?fromPlace='
    url+=str(subway.loc[i,'lat'])+','+str(subway.loc[i,'long'])
    url+='&toPlace='+str(subway.loc[i,'intlat'])+','+str(subway.loc[i,'intlong'])+'&mode=WALK'
    headers={'Accept':'application/json'}
    req=requests.get(url=url,headers=headers)
    js=req.json()
    if list(js.keys())[1]=='error':
        subway.loc[i,'time']=np.nan
    else:
        subway.loc[i,'time']=js['plan']['itineraries'][0]['legs'][0]['duration']
    time.sleep(0.1)
subway.to_csv(path+'subway.csv',index=False)








# Rail
# Intersection points
node=gpd.read_file(path+'node.shp')
node=node['geometry']
node=shapely.geometry.MultiPoint(node)

# Rail points
rail=pd.read_excel(path+'location.xlsx',sheet_name='rail')
rail=gpd.GeoDataFrame(rail,crs={'init': 'epsg:4326'},
                      geometry=[shapely.geometry.Point(xy) for xy in zip(rail['long'], rail['lat'])])
rail=rail['geometry']

# Nearest intersections
nr=[shapely.ops.nearest_points(x,node)[1] for x in rail]
rail=pd.read_excel(path+'location.xlsx',sheet_name='rail')
rail=gpd.GeoDataFrame(rail,crs={'init': 'epsg:4326'},
                      geometry=[shapely.geometry.Point(xy) for xy in zip(rail['long'], rail['lat'])])
rail['intlat']=np.nan
rail['intlong']=np.nan
for i in rail.index:
    rail.loc[i,'intlat']=nr[i].y
    rail.loc[i,'intlong']=nr[i].x
rail=rail[['railid','boro','name','routes','lat','long','intlat','intlong']]
rail.to_csv(path+'rail.csv',index=False)

# Distance from rail points to nearest intersections
frompt=rail.copy()
frompt=gpd.GeoDataFrame(frompt,crs={'init': 'epsg:4326'},
                        geometry=[shapely.geometry.Point(xy) for xy in zip(frompt['long'], frompt['lat'])])
frompt=frompt.to_crs({'init': 'epsg:6539'})
frompt=frompt['geometry']
topt=rail.copy()
topt=gpd.GeoDataFrame(topt,crs={'init': 'epsg:4326'},
                      geometry=[shapely.geometry.Point(xy) for xy in zip(topt['intlong'], topt['intlat'])])
topt=topt.to_crs({'init': 'epsg:6539'})
topt=topt['geometry']
dist=frompt.distance(topt)
rail['distance']=dist
rail.to_csv(path+'rail.csv',index=False)

# Walk time from rail points to nearest intersections
rail['time']=np.nan
for i in rail.index:
    url='http://localhost:8801/otp/routers/default/plan?fromPlace='
    url+=str(rail.loc[i,'lat'])+','+str(rail.loc[i,'long'])
    url+='&toPlace='+str(rail.loc[i,'intlat'])+','+str(rail.loc[i,'intlong'])+'&mode=WALK'
    headers={'Accept':'application/json'}
    req=requests.get(url=url,headers=headers)
    js=req.json()
    if list(js.keys())[1]=='error':
        rail.loc[i,'time']=np.nan
    else:
        rail.loc[i,'time']=js['plan']['itineraries'][0]['legs'][0]['duration']
    time.sleep(0.1)
rail.to_csv(path+'rail.csv',index=False)







# Ferry
# Intersection points
node=gpd.read_file(path+'node.shp')
node=node['geometry']
node=shapely.geometry.MultiPoint(node)

# Ferry points
ferry=pd.read_excel(path+'location.xlsx',sheet_name='ferry')
ferry=gpd.GeoDataFrame(ferry,crs={'init': 'epsg:4326'},
                       geometry=[shapely.geometry.Point(xy) for xy in zip(ferry['long'], ferry['lat'])])
ferry=ferry['geometry']

# Nearest intersections
nr=[shapely.ops.nearest_points(x,node)[1] for x in ferry]
ferry=pd.read_excel(path+'location.xlsx',sheet_name='ferry')
ferry=gpd.GeoDataFrame(ferry,crs={'init': 'epsg:4326'},
                       geometry=[shapely.geometry.Point(xy) for xy in zip(ferry['long'], ferry['lat'])])
ferry['intlat']=np.nan
ferry['intlong']=np.nan
for i in ferry.index:
    ferry.loc[i,'intlat']=nr[i].y
    ferry.loc[i,'intlong']=nr[i].x
ferry=ferry[['ferryid','boro','name','routes','lat','long','intlat','intlong']]
ferry.to_csv(path+'ferry.csv',index=False)

# Distance from ferry points to nearest intersections
frompt=ferry.copy()
frompt=gpd.GeoDataFrame(frompt,crs={'init': 'epsg:4326'},
                        geometry=[shapely.geometry.Point(xy) for xy in zip(frompt['long'], frompt['lat'])])
frompt=frompt.to_crs({'init': 'epsg:6539'})
frompt=frompt['geometry']
topt=ferry.copy()
topt=gpd.GeoDataFrame(topt,crs={'init': 'epsg:4326'},
                      geometry=[shapely.geometry.Point(xy) for xy in zip(topt['intlong'], topt['intlat'])])
topt=topt.to_crs({'init': 'epsg:6539'})
topt=topt['geometry']
dist=frompt.distance(topt)
ferry['distance']=dist
ferry.to_csv(path+'ferry.csv',index=False)

# Walk time from ferry points to nearest intersections
ferry['time']=np.nan
for i in ferry.index:
    url='http://localhost:8801/otp/routers/default/plan?fromPlace='
    url+=str(ferry.loc[i,'lat'])+','+str(ferry.loc[i,'long'])
    url+='&toPlace='+str(ferry.loc[i,'intlat'])+','+str(ferry.loc[i,'intlong'])+'&mode=WALK'
    headers={'Accept':'application/json'}
    req=requests.get(url=url,headers=headers)
    js=req.json()
    if list(js.keys())[1]=='error':
        ferry.loc[i,'time']=np.nan
    else:
        ferry.loc[i,'time']=js['plan']['itineraries'][0]['legs'][0]['duration']
    time.sleep(0.1)
ferry.to_csv(path+'ferry.csv',index=False)













# Xbus
# Intersection points
node=gpd.read_file(path+'node.shp')
node=node['geometry']
node=shapely.geometry.MultiPoint(node)

# Xbus points
xbus=pd.read_excel(path+'location.xlsx',sheet_name='xbus')
xbus=gpd.GeoDataFrame(xbus,crs={'init': 'epsg:4326'},
                      geometry=[shapely.geometry.Point(xy) for xy in zip(xbus['long'], xbus['lat'])])
xbus=xbus['geometry']

# Nearest intersections
nr=[shapely.ops.nearest_points(x,node)[1] for x in xbus]
xbus=pd.read_excel(path+'location.xlsx',sheet_name='xbus')
xbus=gpd.GeoDataFrame(xbus,crs={'init': 'epsg:4326'},
                      geometry=[shapely.geometry.Point(xy) for xy in zip(xbus['long'], xbus['lat'])])
xbus['intlat']=np.nan
xbus['intlong']=np.nan
for i in xbus.index:
    xbus.loc[i,'intlat']=nr[i].y
    xbus.loc[i,'intlong']=nr[i].x
xbus=xbus[['xbusid','name','lat','long','intlat','intlong']]
xbus.to_csv(path+'xbus.csv',index=False)

# Distance from xbus points to nearest intersections
frompt=xbus.copy()
frompt=gpd.GeoDataFrame(frompt,crs={'init': 'epsg:4326'},
                        geometry=[shapely.geometry.Point(xy) for xy in zip(frompt['long'], frompt['lat'])])
frompt=frompt.to_crs({'init': 'epsg:6539'})
frompt=frompt['geometry']
topt=xbus.copy()
topt=gpd.GeoDataFrame(topt,crs={'init': 'epsg:4326'},
                      geometry=[shapely.geometry.Point(xy) for xy in zip(topt['intlong'], topt['intlat'])])
topt=topt.to_crs({'init': 'epsg:6539'})
topt=topt['geometry']
dist=frompt.distance(topt)
xbus['distance']=dist
xbus.to_csv(path+'xbus.csv',index=False)

# Walk time from xbus points to nearest intersections
xbus['time']=np.nan
for i in xbus.index:
    url='http://localhost:8801/otp/routers/default/plan?fromPlace='
    url+=str(xbus.loc[i,'lat'])+','+str(xbus.loc[i,'long'])
    url+='&toPlace='+str(xbus.loc[i,'intlat'])+','+str(xbus.loc[i,'intlong'])+'&mode=WALK'
    headers={'Accept':'application/json'}
    req=requests.get(url=url,headers=headers)
    js=req.json()
    if list(js.keys())[1]=='error':
        xbus.loc[i,'time']=np.nan
    else:
        xbus.loc[i,'time']=js['plan']['itineraries'][0]['legs'][0]['duration']
    time.sleep(0.1)
xbus.to_csv(path+'xbus.csv',index=False)









# PSA
# Intersection points
node=gpd.read_file(path+'node.shp')
node=node['geometry']
node=shapely.geometry.MultiPoint(node)

# PSA points
psa=pd.read_excel(path+'location.xlsx',sheet_name='psa')
psa=gpd.GeoDataFrame(psa,crs={'init': 'epsg:4326'},
                     geometry=[shapely.geometry.Point(xy) for xy in zip(psa['long'], psa['lat'])])
psa=psa['geometry']

# Nearest intersections
nr=[shapely.ops.nearest_points(x,node)[1] for x in psa]
psa=pd.read_excel(path+'location.xlsx',sheet_name='psa')
psa=gpd.GeoDataFrame(psa,crs={'init': 'epsg:4326'},
                     geometry=[shapely.geometry.Point(xy) for xy in zip(psa['long'], psa['lat'])])
psa['intlat']=np.nan
psa['intlong']=np.nan
for i in psa.index:
    psa.loc[i,'intlat']=nr[i].y
    psa.loc[i,'intlong']=nr[i].x
psa=psa[['psaid','name','lat','long','intlat','intlong']]
psa.to_csv(path+'psa.csv',index=False)

# Distance from psa points to nearest intersections
frompt=psa.copy()
frompt=gpd.GeoDataFrame(frompt,crs={'init': 'epsg:4326'},
                        geometry=[shapely.geometry.Point(xy) for xy in zip(frompt['long'], frompt['lat'])])
frompt=frompt.to_crs({'init': 'epsg:6539'})
frompt=frompt['geometry']
topt=psa.copy()
topt=gpd.GeoDataFrame(topt,crs={'init': 'epsg:4326'},
                      geometry=[shapely.geometry.Point(xy) for xy in zip(topt['intlong'], topt['intlat'])])
topt=topt.to_crs({'init': 'epsg:6539'})
topt=topt['geometry']
dist=frompt.distance(topt)
psa['distance']=dist
psa.to_csv(path+'psa.csv',index=False)

# Walk time from psa points to nearest intersections
psa['time']=np.nan
for i in psa.index:
    url='http://localhost:8801/otp/routers/default/plan?fromPlace='
    url+=str(psa.loc[i,'lat'])+','+str(psa.loc[i,'long'])
    url+='&toPlace='+str(psa.loc[i,'intlat'])+','+str(psa.loc[i,'intlong'])+'&mode=WALK'
    headers={'Accept':'application/json'}
    req=requests.get(url=url,headers=headers)
    js=req.json()
    if list(js.keys())[1]=='error':
        psa.loc[i,'time']=np.nan
    else:
        psa.loc[i,'time']=js['plan']['itineraries'][0]['legs'][0]['duration']
    time.sleep(0.1)
psa.to_csv(path+'psa.csv',index=False)







# Adjustment
# Intersection points
node=gpd.read_file(path+'node.shp')
node=node['geometry']
node=shapely.geometry.MultiPoint(node)

# Adjustment points
adj=pd.read_excel(path+'location.xlsx',sheet_name='adj')
adj=gpd.GeoDataFrame(adj,crs={'init': 'epsg:4326'},
                     geometry=[shapely.geometry.Point(xy) for xy in zip(adj['long'], adj['lat'])])
adj=adj['geometry']

# Nearest intersections
nr=[shapely.ops.nearest_points(x,node)[1] for x in adj]
adj=pd.read_excel(path+'location.xlsx',sheet_name='adj')
adj=gpd.GeoDataFrame(adj,crs={'init': 'epsg:4326'},
                     geometry=[shapely.geometry.Point(xy) for xy in zip(adj['long'], adj['lat'])])
adj['intlat']=np.nan
adj['intlong']=np.nan
for i in adj.index:
    adj.loc[i,'intlat']=nr[i].y
    adj.loc[i,'intlong']=nr[i].x
adj=adj[['adjid','name','lat','long','intlat','intlong']]
adj.to_csv(path+'adj.csv',index=False)

# Distance from adjustment points to nearest intersections
frompt=adj.copy()
frompt=gpd.GeoDataFrame(frompt,crs={'init': 'epsg:4326'},
                        geometry=[shapely.geometry.Point(xy) for xy in zip(frompt['long'], frompt['lat'])])
frompt=frompt.to_crs({'init': 'epsg:6539'})
frompt=frompt['geometry']
topt=adj.copy()
topt=gpd.GeoDataFrame(topt,crs={'init': 'epsg:4326'},
                      geometry=[shapely.geometry.Point(xy) for xy in zip(topt['intlong'], topt['intlat'])])
topt=topt.to_crs({'init': 'epsg:6539'})
topt=topt['geometry']
dist=frompt.distance(topt)
adj['distance']=dist
adj.to_csv(path+'adj.csv',index=False)

# Walk time from adjustment points to nearest intersections
adj['time']=np.nan
for i in adj.index:
    url='http://142.93.21.138:8801/otp/routers/default/plan?fromPlace='
    url+=str(adj.loc[i,'lat'])+','+str(adj.loc[i,'long'])
    url+='&toPlace='+str(adj.loc[i,'intlat'])+','+str(adj.loc[i,'intlong'])+'&mode=WALK'
    headers={'Accept':'application/json'}
    req=requests.get(url=url,headers=headers)
    js=req.json()
    if list(js.keys())[1]=='error':
        adj.loc[i,'time']=np.nan
    else:
        adj.loc[i,'time']=js['plan']['itineraries'][0]['legs'][0]['duration']
    time.sleep(0.1)
adj.to_csv(path+'adj.csv',index=False)





