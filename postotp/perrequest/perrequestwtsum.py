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

# Summarize all sites and create images
if __name__=='__main__':
    location=pd.read_excel(path+'waterfront2/input.xlsx',sheet_name='input',dtype=str)
    location['id']=['SITE'+str(x).zfill(4) for x in location['siteid']]
    location['latlong']=[str(x)+','+str(y) for x,y in zip(location['lat'],location['long'])]
    destination=location.loc[0:max(location.count())-1,['id','direction','latlong']].reset_index(drop=True)

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

