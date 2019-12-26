# Travelshed Mapping
# OpenTripPlanner Guide: http://docs.opentripplanner.org/en/latest/
# OpenTripPlanner API Doc: http://dev.opentripplanner.org/apidoc/1.0.0/index.html

import urllib
import requests
import zipfile
import io
import geopandas as gpd
import pandas as pd
import shapely
import datetime

# Create project folder
tvsenv='C:/Users/Yijun Ma/Desktop/travelshedrevamp'
otpenv=tvsenv+'/otp12'
shpenv=tvsenv+'/quadstate'



# Download OpenTripPlanner jar file
# Double check the update before downloading: https://repo1.maven.org/maven2/org/opentripplanner/otp/
url='https://repo1.maven.org/maven2/org/opentripplanner/otp/1.2.0/otp-1.2.0-shaded.jar'
urllib.request.urlretrieve(url,otpenv+'/otp-1.2.0-shaded.jar')






# Download OpenStreetMap pbf file from geofabrik
# New York
url='https://download.geofabrik.de/north-america/us/new-york-latest.osm.pbf'
urllib.request.urlretrieve(url,otpenv+'/newyork.osm.pbf')
# New Jersey
url='https://download.geofabrik.de/north-america/us/new-jersey-latest.osm.pbf'
urllib.request.urlretrieve(url,otpenv+'/newjersey.osm.pbf')
# Connecticut
url='https://download.geofabrik.de/north-america/us/connecticut-latest.osm.pbf'
urllib.request.urlretrieve(url,otpenv+'/connecticut.osm.pbf')
# Pennsylvania
url='https://download.geofabrik.de/north-america/us/pennsylvania-latest.osm.pbf'
urllib.request.urlretrieve(url,otpenv+'/pennsylvania.osm.pbf')



# Download GTFS data
# New York
# MTA NYCT Subway
url='http://web.mta.info/developers/data/nyct/subway/google_transit.zip'
urllib.request.urlretrieve(url,otpenv+'/mtanyctsubway.zip')
# MTA NYCT Bus Bronx
url='http://web.mta.info/developers/data/nyct/bus/google_transit_bronx.zip'
urllib.request.urlretrieve(url,otpenv+'/mtanyctbusbronx.zip')
# MTA NYCT Bus Brooklyn
url='http://web.mta.info/developers/data/nyct/bus/google_transit_brooklyn.zip'
urllib.request.urlretrieve(url,otpenv+'/mtanyctbusbrooklyn.zip')
# MTA NYCT Bus Manhattan
url='http://web.mta.info/developers/data/nyct/bus/google_transit_manhattan.zip'
urllib.request.urlretrieve(url,otpenv+'/mtanyctbusmanhattan.zip')
# MTA NYCT Bus Queens
url='http://web.mta.info/developers/data/nyct/bus/google_transit_queens.zip'
urllib.request.urlretrieve(url,otpenv+'/mtanyctbusqueens.zip')
# MTA NYCT Bus Staten Island
url='http://web.mta.info/developers/data/nyct/bus/google_transit_staten_island.zip'
urllib.request.urlretrieve(url,otpenv+'/mtanyctbusstatenisland.zip')
# MTA Bus Company
url='http://web.mta.info/developers/data/busco/google_transit.zip'
urllib.request.urlretrieve(url,otpenv+'/mtabusco.zip')
# MTA long Island Railroad
url='http://web.mta.info/developers/data/lirr/google_transit.zip'
urllib.request.urlretrieve(url,otpenv+'/mtalirr.zip')
# MTA Metro-North Railroad
url='http://web.mta.info/developers/data/mnr/google_transit.zip'
urllib.request.urlretrieve(url,otpenv+'/mtamnr.zip')
# Port Authority Trans-Hudson
url='http://data.trilliumtransit.com/gtfs/path-nj-us/path-nj-us.zip'
urllib.request.urlretrieve(url,otpenv+'/path.zip')
# JFK AirTrain (manually download the 7 April 2015 version and change the calendar)
print('https://transitfeeds.com/p/jfk-airtrain/433')
# NYC DOT Staten Island Ferry
url='http://www.nyc.gov/html/dot/downloads/misc/siferry-gtfs.zip'
urllib.request.urlretrieve(url,otpenv+'/nycdotsiferry.zip')
# NYC Ferry
url='http://nycferry.connexionz.net/rtt/public/utility/gtfs.aspx?google_transit.zip'
urllib.request.urlretrieve(url,otpenv+'/nycferry.zip')
# NY Waterway
url='https://s3.amazonaws.com/data.bytemark.co/nywaterway/nywaterway.zip'
urllib.request.urlretrieve(url,otpenv+'/nywaterway.zip')
# Seastreak (manually download the file due to certificate issue)
print('http://seastreak.com/api/transit/google_transit.zip')
# NYC Downtown Alliance
url='http://mjcaction.com/MJC_GTFS_Public/downtown_nyc_google_transit.zip'
urllib.request.urlretrieve(url,otpenv+'/downtownalliance.zip')
# Nassau Inter-County Express Bus (manually download the file for the most up to date version)
print('http://www.nicebus.com/Passenger-Information/App-Developers.aspx')
# Suffolk County Transit (http://www.suffolkcountyny.gov/Departments/PublicWorks/Transportation/GeneralTransitFeedSpecificationGTFSDetails.aspx)
url='http://www.suffolkcountyny.gov/portals/0/publicworks/zip/google_transit.zip'
urllib.request.urlretrieve(url,otpenv+'/suffolk.zip')
# Westchester County Bee-Line System (NYS511: https://511ny.org/developers/resources)
url='https://s3.amazonaws.com/datatools-nysdot/public/Westchester_County_Bee-Line_System.zip'
urllib.request.urlretrieve(url,otpenv+'/beeline.zip')
# TappanZee Express (Rockland TZX; NYS511: https://511ny.org/developers/resources)
url='https://s3.amazonaws.com/datatools-nysdot/public/TappanZee_Express.zip'
urllib.request.urlretrieve(url,otpenv+'/tzx.zip')
# Ulster County Area Transit (NYS511: https://511ny.org/developers/resources)
url='https://s3.amazonaws.com/datatools-nysdot/public/UCAT_Ulster_County_Area_Transit.zip'
urllib.request.urlretrieve(url,otpenv+'/ucat.zip')
# Capital District Transportation Authority
url='http://www.cdta.org/schedules/google_transit.zip'
urllib.request.urlretrieve(url,otpenv+'/cdta.zip')
# Rochester-Genesee Regional Transportation Authority (NYS511: https://511ny.org/developers/resources)
url='http://scheduledata.rgrta.com/google_transit_merged.zip'
urllib.request.urlretrieve(url,otpenv+'/rgrta.zip')
# Niagara Frontier Transportation Authority
url='http://www.nfta.com/metro/__googletransit/google_transit.zip'
urllib.request.urlretrieve(url,otpenv+'/nfta.zip')

# New Jersey
# New Jersey Transit (manually download the file; need to log in)
print('https://www.njtransit.com/mt/mt_servlet.srv?hdnPageAction=MTDevResourceDownloadTo&Category=rail')
print('https://www.njtransit.com/mt/mt_servlet.srv?hdnPageAction=MTDevResourceDownloadTo&Category=bus')

# Connecticut
# Connecticut Transit Hartford-New Haven-New Britain-Waterbury-Meriden
url='https://www.cttransit.com/sites/default/files/gtfs/googlect_transit.zip'
urllib.request.urlretrieve(url,otpenv+'/cthfnhnbwbmd.zip')
# Connecticut Transit Stamford
url='https://www.cttransit.com/sites/default/files/gtfs/googlestam_transit.zip'
urllib.request.urlretrieve(url,otpenv+'/ctstam.zip')
# Shore Line East
url='http://www.shorelineeast.com/google_transit.zip'
urllib.request.urlretrieve(url,otpenv+'/sle.zip')
# 9 Town Transit
url='http://data.trilliumtransit.com/gtfs/ninetown-connecticut-us/ninetown-connecticut-us.zip'
urllib.request.urlretrieve(url,otpenv+'/9town.zip')
# Norwalk Transit District
url='https://www.norwalktransit.com/s/GTFS_Data.zip'
urllib.request.urlretrieve(url,otpenv+'/norwalk.zip')

# Pennsylvania
# Port Authority Transit Corporation
url='http://www.ridepatco.org/developers/PortAuthorityTransitCorporation.zip'
urllib.request.urlretrieve(url,otpenv+'/patco.zip')
# Southeastern Pennsylvania Transportation Authority (manually download the file and separate bus and rail)
print('http://www3.septa.org/') # Media=>Developer=>GTFS=>github
# Monroe County Transit Authority (Pocono Pony)
url='https://www.gomcta.com/google_transit.zip'
urllib.request.urlretrieve(url,otpenv+'/pocono.zip')
# Rabbit Transit
url='http://www.rabbittransit.org/infopoint/gtfs/google_transit.zip'
urllib.request.urlretrieve(url,otpenv+'/rabbit.zip')
# Centre County Transit Authority (CATA)
url='https://catabus.com/data/google_transit.zip'
urllib.request.urlretrieve(url,otpenv+'/cata.zip')
# Port Authority of Allegheny County
url='http://www.portauthority.org/GeneralTransitFeed/google_transit.zip'
urllib.request.urlretrieve(url,otpenv+'/paac.zip')
# Erie Metropolitan Transit Authority (EMTA) (manually download the file for the most up to date version)
print('http://www.ride-the-e.com/wp-content/uploads/2017/09/EMTA_GTFS.zip')

# Rhode Island (manually download the file for the most up to date version)
# Rhode Island Public Transit Authority (RIPTA)
print('https://www.ripta.com/mobile-applications')

# Delaware
# Delaware Transit Corporation (DART)
url='https://dartfirststate.com/information/routes/gtfs_data/dartfirststate_de_us.zip'
urllib.request.urlretrieve(url,otpenv+'/dart.zip')

# Amtrak (transitland FOIA)
url='http://github.com/transitland/gtfs-archives-not-hosted-elsewhere/raw/master/amtrak.zip'
urllib.request.urlretrieve(url,otpenv+'/amtrak.zip')

# Problematic feeds
# Centro (NYS511: https://511ny.org/developers/resources)
print('https://www.centro.org/CentroGTFS/CentroGTFS.zip')
# Lehigh and Northampton Transportation Authority (LANTA)
print('https://github.com/LANTA-Transportation-Authority/GTFS-data/raw/master/lanta_gtfs_feed.zip')



# Download Census Block shapefiles and convert to centorids
# New York
url='ftp://ftp2.census.gov/geo/tiger/TIGER2017/TABBLOCK/tl_2017_36_tabblock10.zip'
urllib.request.urlretrieve(url,tvsenv+'/newyorkblock.zip')
zip_ref=zipfile.ZipFile(tvsenv+'/newyorkblock.zip','r')
zip_ref.extractall(tvsenv+'/newyorkblock')
zip_ref.close()
# New Jersey
url='ftp://ftp2.census.gov/geo/tiger/TIGER2017/TABBLOCK/tl_2017_34_tabblock10.zip'
urllib.request.urlretrieve(url,tvsenv+'/newjerseyblock.zip')
zip_ref=zipfile.ZipFile(tvsenv+'/newjerseyblock.zip','r')
zip_ref.extractall(tvsenv+'/newjerseyblock')
zip_ref.close()
# Connecticut
url='ftp://ftp2.census.gov/geo/tiger/TIGER2017/TABBLOCK/tl_2017_09_tabblock10.zip'
urllib.request.urlretrieve(url,tvsenv+'/connecticutblock.zip')
zip_ref=zipfile.ZipFile(tvsenv+'/connecticutblock.zip','r')
zip_ref.extractall(tvsenv+'/connecticutblock')
zip_ref.close()
# Pennsylvania
url='ftp://ftp2.census.gov/geo/tiger/TIGER2017/TABBLOCK/tl_2017_42_tabblock10.zip'
urllib.request.urlretrieve(url,tvsenv+'/pennsylvaniablock.zip')
zip_ref=zipfile.ZipFile(tvsenv+'/pennsylvaniablock.zip','r')
zip_ref.extractall(tvsenv+'/pennsylvaniablock')
zip_ref.close()
# Merge shapefiles
ny=gpd.read_file(tvsenv+'/newyorkblock/tl_2017_36_tabblock10.shp')
nj=gpd.read_file(tvsenv+'/newjerseyblock/tl_2017_34_tabblock10.shp')
ct=gpd.read_file(tvsenv+'/connecticutblock/tl_2017_09_tabblock10.shp')
pa=gpd.read_file(tvsenv+'/pennsylvaniablock/tl_2017_42_tabblock10.shp')
bk=gpd.GeoDataFrame()
bk=bk.append(ny,ignore_index=True)
bk=bk.append(nj,ignore_index=True)
bk=bk.append(ct,ignore_index=True)
bk=bk.append(pa,ignore_index=True)
bk.to_file(filename=shpenv+'/quadstateblock.shp',driver='ESRI Shapefile') # crs: 4269
bkpt=bk
bkpt['LAT']=pd.to_numeric(bkpt['INTPTLAT10'])
bkpt['LONG']=pd.to_numeric(bkpt['INTPTLON10'])
bkpt=bkpt[['GEOID10','LAT','LONG']]
bkpt=gpd.GeoDataFrame(bkpt,crs={'init': 'epsg:4326'},geometry=[shapely.geometry.Point(xy) for xy in zip(bkpt.LONG, bkpt.LAT)])
bkpt.to_file(filename=shpenv+'/quadstateblockpoint.shp',driver='ESRI Shapefile')




# Set up OpenTripPlanner in cmd
print('java -Xmx8G -jar "'+otpenv+'/otp-1.2.0-shaded.jar" --build "'+otpenv+'" --inMemory --analyst --port 8801 --securePort 8802')


print(datetime.datetime.now())
# Create travelshed table
# Set destination
destination='40.684913,-73.978065'

# Set typical day
typicaldate='2018/06/06'

# Create arrival time list
arrivaltimeinterval=5 # in minutes
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

# Set maximum pre transit free flow driving time
maxPreTransitTime=5 # in minutes

# Set cut off points between 0-120 mins
cutoffinterval=2 # in minutes
cutoffstart=0
cutoffend=120
cutoffincrement=cutoffstart
cutoff=''
while cutoffincrement<cutoffend:
    cutoff+='&cutoffSec='+str((cutoffincrement+cutoffinterval)*60)
    cutoffincrement+=cutoffinterval

# Generate walk and transit isochrones
for i in arrivaltime:
    url='http://localhost:8801/otp/routers/default/isochrone?batch=true&mode=WALK,TRANSIT&fromPlace='+destination+'&toPlace='+destination
    url+='&arriveBy=true&date='+typicaldate+'&time='+i+'&maxTransfers='+str(maxTransfers)+'&maxWalkDistance='+str(maxWalkDistance)+cutoff
    headers={'Accept':'application/json'}
    req=requests.get(url=url,headers=headers)
    file = open(tvsenv+'/quadstate/wt'+i[0:2]+i[3:5]+'.geojson', "w")
    file.write(req.text)
    file.close()

# Generate park and ride isochrones
for i in arrivaltime:
    url='http://localhost:8801/otp/routers/default/isochrone?batch=true&mode=CAR,WALK,TRANSIT&fromPlace='+destination+'&toPlace='+destination
    url+='&arriveBy=true&date='+typicaldate+'&time='+i+'&maxTransfers='+str(maxTransfers)+'&maxWalkDistance='+str(maxWalkDistance)+'&maxPreTransitTime='+str(maxPreTransitTime*60)+cutoff
    headers={'Accept':'application/json'}
    req=requests.get(url=url,headers=headers)
    file = open(tvsenv+'/quadstate/pr'+i[0:2]+i[3:5]+'.geojson', "w")
    file.write(req.text)
    file.close()



# Join isochrones to the block centroids
bkpt=gpd.read_file(tvsenv+'/quadstate/quadstateblockpoint.shp')
bkpt.crs={'init': 'epsg:4326'}
for i in arrivaltime:
    iso=gpd.read_file(tvsenv+'/quadstate/'+i[0:2]+i[3:5]+'.geojson')
    bkpt['T'+i[0:2]+i[3:5]]=999
    cut=[999]+list(range(cutoffend,cutoffstart,-cutoffinterval))
    for j in range(0,(len(cut)-1)):
        if (iso.loc[iso.time==cut[j+1]*60,'geometry'].notna()).bool():
            bkptiso=gpd.sjoin(bkpt.loc[bkpt['T'+i[0:2]+i[3:5]]==cut[j]],iso.loc[iso.time==cut[j+1]*60],how='left',op='within')
            bkptiso=bkptiso.loc[pd.notnull(bkptiso.time),'GEOID10']
            bkpt.loc[bkpt.GEOID10.isin(bkptiso),'T'+i[0:2]+i[3:5]]=cut[j+1]
        else:
            print(i+' '+str(cut[j+1])+'-minute isochrone has no geometry!')
print(datetime.datetime.now())

bkpt.to_file(filename=shpenv+'/wt.shp',driver='ESRI Shapefile')
























# Download isochrones in zipfile
url='http://localhost:8080/otp/routers/default/isochrone?batch=true&mode=WALK,TRANSIT&fromPlace='+destination+'&toPlace='+destination
url+='&arriveBy=true&date='+typicaldate+'&time='+i+'&maxTransfers='+maxTransfers+'&maxWalkDistance='+maxWalkDistance+cutoff
headers={'Accept':'application/x-zip-compressed'}
req=requests.get(url=url,headers=headers)
zip_ref=zipfile.ZipFile(io.BytesIO(req.content))
zip_ref.extractall(tvsenv+'/quadstate/0700')
zip_ref.close()






























