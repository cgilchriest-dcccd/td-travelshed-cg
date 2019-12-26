import geopandas as gpd
import pandas as pd
import shapely

path='D:/TRAVELSHEDREVAMP/'
pd.set_option('display.max_columns', None)
nyc=['36005','36047','36061','36081','36085']



ct=gpd.read_file(path+'shp/quadstatectclipped.shp')
ct=ct[[x[0:5] in nyc for x in ct['tractid']]]

ctpt=ct.drop('geometry',axis=1)
ctpt=gpd.GeoDataFrame(ctpt,geometry=[shapely.geometry.Point(xy) for xy in zip(ctpt['long'],ctpt['lat'])],crs={'init': 'epsg:4326'})

cd=gpd.read_file(path+'hpd2/nycdwi.shp')
cd=cd.to_crs({'init': 'epsg:4326'})
cd['BOROCD']=[str(int(x)) for x in cd['BoroCD']]
cd=cd.drop(['BoroCD','Shape_Leng','Shape_Area'],axis=1)
cd=cd[[x not in ['164','226','227','228','355','356','480','481','482','483','484','595'] for x in cd['BOROCD']]]

ctcd=gpd.sjoin(ctpt,cd,how='left',op='intersects')
ctcd=ctcd[['tractid','BOROCD']]

pop=pd.read_csv(path+'hpd2/ctacspop.csv',dtype=str)

ctcdpop=pd.merge(ctcd,pop,on='tractid',how='left')
ctcdpop=ctcdpop.dropna(axis=0)
ctcdpop.columns=['tractid','BOROCD','pop']
ctcdpop['tractid']='RES'+ctcdpop['tractid']

resbkgravity=pd.read_csv(path+'hpd2/resbkgravity2.csv',dtype=str)
resbkgravity=resbkgravity[['Unnamed: 0','GRAVITYWAC']]
resbkgravity.columns=['tractid','GRAVITYWAC']

rescdgravity=pd.merge(resbkgravity,ctcdpop,on='tractid',how='left')
rescdgravity=rescdgravity.dropna(axis=0)
rescdgravity['GRAVITYWAC']=pd.to_numeric(rescdgravity['GRAVITYWAC'])
rescdgravity['pop']=pd.to_numeric(rescdgravity['pop'])
rescdgravity['gravpop']=rescdgravity['GRAVITYWAC']*rescdgravity['pop']
rescdgravity=rescdgravity.groupby('BOROCD',as_index=False).agg({'gravpop':'sum','pop':'sum'})
rescdgravity['GRAVITYWAC']=rescdgravity['gravpop']/rescdgravity['pop']
rescdgravity=rescdgravity[['BOROCD','GRAVITYWAC']]
rescdgravity=rescdgravity.dropna(axis=0)

cdclip=gpd.read_file(path+'hpd2/nycd.shp')
cdclip=cdclip.to_crs({'init': 'epsg:4326'})
cdclip['BOROCD']=[str(int(x)) for x in cdclip['BoroCD']]
cdclip=cdclip.drop(['BoroCD','Shape_Leng','Shape_Area'],axis=1)

rescdgravity=pd.merge(cdclip,rescdgravity,on='BOROCD',how='inner')
rescdgravity.to_file(path+'hpd2/hpd2.shp')
rescdgravity=rescdgravity.drop('geometry',axis=1)
rescdgravity.to_csv(path+'hpd2/hpd2.csv',index=False)
