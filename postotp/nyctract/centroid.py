import pandas as pd
import geopandas as gpd
import shapely
import numpy as np
import requests
import time


# NYC Census Tracts
# Res
# Intersection points
path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2018/TRAVELSHEDREVAMP/'

node=gpd.read_file(path+'location/node.shp')
node=node['geometry']
node=shapely.geometry.MultiPoint(node)

# Res Census Tract points
resct=pd.read_excel(path+'nyctract/centroid/centroid.xlsx',sheet_name='tractpt',dtype=str)
for i in resct.columns[1:]:
    resct.loc[:,i]=pd.to_numeric(resct.loc[:,i])
resct=gpd.GeoDataFrame(resct,crs={'init': 'epsg:4326'},
                       geometry=[shapely.geometry.Point(xy) for xy in zip(resct['reslong'], resct['reslat'])])
resctgeom=resct['geometry']

# Nearest intersections
nr=[shapely.ops.nearest_points(x,node)[1] for x in resctgeom]
resct['resintlat']=np.nan
resct['resintlong']=np.nan
for i in resct.index:
    resct.loc[i,'resintlat']=nr[i].y
    resct.loc[i,'resintlong']=nr[i].x
resct=resct[['censustract','lat','long','reslat','reslong','resintlat','resintlong']]
resct.to_csv(path+'nyctract/centroid/nycrestractpt.csv',index=False)

# Distance from Res Census Tract points to nearest intersections
frompt=resct.copy()
frompt=gpd.GeoDataFrame(frompt,crs={'init': 'epsg:4326'},
                        geometry=[shapely.geometry.Point(xy) for xy in zip(frompt['reslong'], frompt['reslat'])])
frompt=frompt.to_crs({'init': 'epsg:6539'})
frompt=frompt['geometry']
topt=resct.copy()
topt=gpd.GeoDataFrame(topt,crs={'init': 'epsg:4326'},
                      geometry=[shapely.geometry.Point(xy) for xy in zip(topt['resintlong'], topt['resintlat'])])
topt=topt.to_crs({'init': 'epsg:6539'})
topt=topt['geometry']
dist=frompt.distance(topt)
resct['resintresdistance']=dist
resct.to_csv(path+'nyctract/centroid/nycrestractpt.csv',index=False)

# Walk time from Res Census Tract points to nearest intersections
resct['resintrestime']=np.nan
for i in resct.index:
    url='http://142.93.21.138:8801/otp/routers/default/plan?fromPlace='
    url+=str(resct.loc[i,'reslat'])+','+str(resct.loc[i,'reslong'])
    url+='&toPlace='+str(resct.loc[i,'resintlat'])+','+str(resct.loc[i,'resintlong'])+'&mode=WALK'
    headers={'Accept':'application/json'}
    req=requests.get(url=url,headers=headers)
    js=req.json()
    if list(js.keys())[1]=='error':
        resct.loc[i,'resintrestime']=np.nan
    else:
        resct.loc[i,'resintrestime']=js['plan']['itineraries'][0]['legs'][0]['duration']
    time.sleep(0.1)
resct.to_csv(path+'nyctract/centroid/nycrestractpt.csv',index=False)

# Distance from Res Census Tract intersections to orignal centroids
frompt=resct.copy()
frompt=gpd.GeoDataFrame(frompt,crs={'init': 'epsg:4326'},
                        geometry=[shapely.geometry.Point(xy) for xy in zip(frompt['resintlong'], frompt['resintlat'])])
frompt=frompt.to_crs({'init': 'epsg:6539'})
frompt=frompt['geometry']
topt=resct.copy()
topt=gpd.GeoDataFrame(topt,crs={'init': 'epsg:4326'},
                      geometry=[shapely.geometry.Point(xy) for xy in zip(topt['long'], topt['lat'])])
topt=topt.to_crs({'init': 'epsg:6539'})
topt=topt['geometry']
dist=frompt.distance(topt)
resct['resintorgdistance']=dist
resct.to_csv(path+'nyctract/centroid/nycrestractpt.csv',index=False)

# Walk time from Res Census Tract intersections to orignal centroids
resct['resintorgtime']=np.nan
for i in resct.index:
    url='http://142.93.21.138:8801/otp/routers/default/plan?fromPlace='
    url+=str(resct.loc[i,'resintlat'])+','+str(resct.loc[i,'resintlong'])
    url+='&toPlace='+str(resct.loc[i,'lat'])+','+str(resct.loc[i,'long'])+'&mode=WALK'
    headers={'Accept':'application/json'}
    req=requests.get(url=url,headers=headers)
    js=req.json()
    if list(js.keys())[1]=='error':
        resct.loc[i,'resintorgtime']=np.nan
    else:
        resct.loc[i,'resintorgtime']=js['plan']['itineraries'][0]['legs'][0]['duration']
    time.sleep(0.1)
resct.to_csv(path+'nyctract/centroid/nycrestractpt.csv',index=False)



# Work
# Intersection points
path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2018/TRAVELSHEDREVAMP/'

node=gpd.read_file(path+'location/node.shp')
node=node['geometry']
node=shapely.geometry.MultiPoint(node)

# Work Census Tract points
workct=pd.read_excel(path+'nyctract/centroid/centroid.xlsx',sheet_name='tractpt',dtype=str)
for i in workct.columns[1:]:
    workct.loc[:,i]=pd.to_numeric(workct.loc[:,i])
workct=gpd.GeoDataFrame(workct,crs={'init': 'epsg:4326'},
                       geometry=[shapely.geometry.Point(xy) for xy in zip(workct['worklong'], workct['worklat'])])
workctgeom=workct['geometry']

# Nearest intersections
nr=[shapely.ops.nearest_points(x,node)[1] for x in workctgeom]
workct['workintlat']=np.nan
workct['workintlong']=np.nan
for i in workct.index:
    workct.loc[i,'workintlat']=nr[i].y
    workct.loc[i,'workintlong']=nr[i].x
workct=workct[['censustract','lat','long','worklat','worklong','workintlat','workintlong']]
workct.to_csv(path+'nyctract/centroid/nycworktractpt.csv',index=False)

# Distance from Work Census Tract points to nearest intersections
frompt=workct.copy()
frompt=gpd.GeoDataFrame(frompt,crs={'init': 'epsg:4326'},
                        geometry=[shapely.geometry.Point(xy) for xy in zip(frompt['worklong'], frompt['worklat'])])
frompt=frompt.to_crs({'init': 'epsg:6539'})
frompt=frompt['geometry']
topt=workct.copy()
topt=gpd.GeoDataFrame(topt,crs={'init': 'epsg:4326'},
                      geometry=[shapely.geometry.Point(xy) for xy in zip(topt['workintlong'], topt['workintlat'])])
topt=topt.to_crs({'init': 'epsg:6539'})
topt=topt['geometry']
dist=frompt.distance(topt)
workct['workintworkdistance']=dist
workct.to_csv(path+'nyctract/centroid/nycworktractpt.csv',index=False)

# Walk time from Work Census Tract points to nearest intersections
workct['workintworktime']=np.nan
for i in workct.index:
    url='http://142.93.21.138:8801/otp/routers/default/plan?fromPlace='
    url+=str(workct.loc[i,'worklat'])+','+str(workct.loc[i,'worklong'])
    url+='&toPlace='+str(workct.loc[i,'workintlat'])+','+str(workct.loc[i,'workintlong'])+'&mode=WALK'
    headers={'Accept':'application/json'}
    req=requests.get(url=url,headers=headers)
    js=req.json()
    if list(js.keys())[1]=='error':
        workct.loc[i,'workintworktime']=np.nan
    else:
        workct.loc[i,'workintworktime']=js['plan']['itineraries'][0]['legs'][0]['duration']
    time.sleep(0.1)
workct.to_csv(path+'nyctract/centroid/nycworktractpt.csv',index=False)

# Distance from Work Census Tract intersections to orignal centroids
frompt=workct.copy()
frompt=gpd.GeoDataFrame(frompt,crs={'init': 'epsg:4326'},
                        geometry=[shapely.geometry.Point(xy) for xy in zip(frompt['workintlong'], frompt['workintlat'])])
frompt=frompt.to_crs({'init': 'epsg:6539'})
frompt=frompt['geometry']
topt=workct.copy()
topt=gpd.GeoDataFrame(topt,crs={'init': 'epsg:4326'},
                      geometry=[shapely.geometry.Point(xy) for xy in zip(topt['long'], topt['lat'])])
topt=topt.to_crs({'init': 'epsg:6539'})
topt=topt['geometry']
dist=frompt.distance(topt)
workct['workintorgdistance']=dist
workct.to_csv(path+'nyctract/centroid/nycworktractpt.csv',index=False)

# Walk time from Work Census Tract intersections to orignal centroids
workct['workintorgtime']=np.nan
for i in workct.index:
    url='http://142.93.21.138:8801/otp/routers/default/plan?fromPlace='
    url+=str(workct.loc[i,'workintlat'])+','+str(workct.loc[i,'workintlong'])
    url+='&toPlace='+str(workct.loc[i,'lat'])+','+str(workct.loc[i,'long'])+'&mode=WALK'
    headers={'Accept':'application/json'}
    req=requests.get(url=url,headers=headers)
    js=req.json()
    if list(js.keys())[1]=='error':
        workct.loc[i,'workintorgtime']=np.nan
    else:
        workct.loc[i,'workintorgtime']=js['plan']['itineraries'][0]['legs'][0]['duration']
    time.sleep(0.1)
workct.to_csv(path+'nyctract/centroid/nycworktractpt.csv',index=False)

