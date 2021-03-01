import geopandas as gpd
import pandas as pd
import numpy as np
import dask.dataframe as dd
import plotly.graph_objects as go
import plotly.express as px
import plotly.io as pio
import json

path='C:/Users/mayij/Desktop/DOC/DCP2018/TRAVELSHEDREVAMP/'
pio.renderers.default = "browser"


df=dd.read_csv(path+'nyctract/resbk3.csv',dtype=float,converters={'blockid':str})
df=df.loc[df['RES36005039500']<=15,['blockid','RES36005039500']].reset_index(drop=True)
df=df.compute()
wtbk=gpd.read_file(path+'shp/quadstatebkclipped.shp')
wtbk.crs=4326
wtbk=wtbk[['blockid','geometry']].reset_index(drop=True)
df=pd.merge(wtbk,df,how='inner',on='blockid')
bkgjs=json.loads(df.to_json())



p=go.Figure(go.Choroplethmapbox(geojson=bkgjs,
                                featureidkey='properties.blockid',
                                locations=df['blockid'],
                                z=df['RES36005039500'],
                                zmin=0,
                                zmax=15,
                                colorscale='Blues',
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
                                          'tickvals':[0,5,10,15],
                                          'tickfont_size':12,
                                          'bgcolor':'rgba(255,255,255,0.5)'},
                                marker={'line_color':'white',
                                        'line_width':0.5,
                                        'opacity':0.8},
                                hovertext='<b>Census Block: </b>'+
                                          df['blockid']+
                                          '<br>'+
                                          '<b>Transit Travel Time (mins): </b>'+
                                          df['RES36005039500'].astype(int).astype(str),
                                hoverinfo='text'))
p=p.add_trace(go.Scattermapbox(lat=[40.84964254],
                   lon=[-73.89659635],
                   mode='markers',
                   marker={'size':10,
                           'color':'black'},
                   hoverinfo='none'))
p.update_layout(mapbox={'style':'carto-positron',
                        'center':{'lat':np.mean([min(df.bounds['miny']),max(df.bounds['maxy'])]),
                                'lon':np.mean([min(df.bounds['minx']),max(df.bounds['maxx'])])},
                        'zoom':14.5},
                title={'text':'<b>15-min Transit Travelshed from Census Tract 365 in the Bronx</b>',
                       'font_size':20},
                template='ggplot2',
                font={'family':'arial',
                      'color':'black'},
                margin={'r':0,'t':40,'l':0,'b':0})
p.show()
p.write_html('C:/Users/mayij/Desktop/DOC/GITHUB/td-plotly/15min.html',include_plotlyjs='cdn')










