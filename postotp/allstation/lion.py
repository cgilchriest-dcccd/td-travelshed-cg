import pandas as pd
import geopandas as gpd


path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2017/TRAVELSHED/travelshedrevamp/location/'


# LION line: "SegmentTyp" IN ('B','R','T','C','S','U') AND "FeatureTyp" IN ('0','6','C') AND "RW_TYPE"=' 1' AND "NonPed"!='V' AND "TrafDir"!='P'
# Save lionline shapefile with WGS84 projection and only physicalid, seqnum, nodeidfrom, nodeidto

# LION Node: "VIntersect"!='VirtualIntersection'
# Save lionnode shapefile with WGS84 projection and only nodeid


lionline=gpd.read_file(path+'lionline.shp')
lionline['physicalid']=pd.to_numeric(lionline['PhysicalID'],errors='coerce')
lionline['seqnum']=pd.to_numeric(lionline['SeqNum'],errors='coerce')
lionline['fromnode']=pd.to_numeric(lionline['NodeIDFrom'],errors='coerce')
lionline['tonode']=pd.to_numeric(lionline['NodeIDTo'],errors='coerce')
lionline=lionline[['physicalid','seqnum','fromnode','tonode']]
lionline=lionline.sort_values(['physicalid','seqnum'])

fromnode=lionline.drop_duplicates('physicalid',keep='first')
fromnode=fromnode[['physicalid','fromnode']]
tonode=lionline.drop_duplicates('physicalid',keep='last')
tonode=tonode[['physicalid','tonode']]
node=pd.merge(fromnode,tonode,how='inner',on='physicalid')
node=node['fromnode'].append(node['tonode'])
node=node.drop_duplicates(keep='first')

lionnode=gpd.read_file(path+'lionnode.shp')
df=lionnode.loc[lionnode['NODEID'].isin(node),:]
df.to_file(path+'node.shp')
