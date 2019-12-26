import pandas as pd

path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2017/TRAVELSHED/travelshedrevamp/location/google_transit_bronx/'
stoptime=pd.read_csv(path+'stop_times.txt')
trip=pd.read_csv(path+'trips.txt')
stop=pd.read_csv(path+'stops.txt')
df=pd.merge(stoptime,trip,how='outer',on='trip_id')
df=pd.merge(df,stop,how='outer',on='stop_id')
df=df[['stop_id','stop_name','stop_lat','stop_lon','route_id']]
df=df.drop_duplicates(keep='first')
df.to_csv(path+'df.csv',index=False)


path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2017/TRAVELSHED/travelshedrevamp/location/google_transit_brooklyn/'
trip=pd.read_csv(path+'trips.txt')
trip=trip.loc[[x in ['X27','X28','X37','X38'] for x in trip['route_id']],:]
trip=trip.loc[[x in ['UP_C8-Weekday-SDon'] for x in trip['service_id']],:]
trip=trip.loc[[x in [1] for x in trip['direction_id']],:]
trip=list(trip['trip_id'].drop_duplicates(keep='first'))
stoptime=pd.read_csv(path+'stop_times.txt')
stoptime=stoptime.loc[[x in trip for x in stoptime['trip_id']],:]
stoptime=list(stoptime['stop_id'].drop_duplicates(keep='first'))
stop=pd.read_csv(path+'stops.txt')
stop=stop.loc[[x in stoptime for x in stop['stop_id']],:]
stop=stop[['stop_id','stop_name','stop_lat','stop_lon']].drop_duplicates(keep='first')
stop.to_csv(path+'stop.csv',index=False)





df=pd.merge(stoptime,trip,how='outer',on='trip_id')
df=pd.merge(df,stop,how='outer',on='stop_id')
df=df[['stop_id','stop_name','stop_lat','stop_lon','route_id']]
df=df.drop_duplicates(keep='first')
df.to_csv(path+'df.csv',index=False)


path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2017/TRAVELSHED/travelshedrevamp/location/google_transit_manhattan/'
stoptime=pd.read_csv(path+'stop_times.txt')
trip=pd.read_csv(path+'trips.txt')
stop=pd.read_csv(path+'stops.txt')
df=pd.merge(stoptime,trip,how='outer',on='trip_id')
df=pd.merge(df,stop,how='outer',on='stop_id')
df=df[['stop_id','stop_name','stop_lat','stop_lon','route_id']]
df=df.drop_duplicates(keep='first')
df.to_csv(path+'df.csv',index=False)


path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2017/TRAVELSHED/travelshedrevamp/location/google_transit_queens/'
stoptime=pd.read_csv(path+'stop_times.txt')
trip=pd.read_csv(path+'trips.txt')
stop=pd.read_csv(path+'stops.txt')
df=pd.merge(stoptime,trip,how='outer',on='trip_id')
df=pd.merge(df,stop,how='outer',on='stop_id')
df=df[['stop_id','stop_name','stop_lat','stop_lon','route_id']]
df=df.drop_duplicates(keep='first')
df.to_csv(path+'df.csv',index=False)



path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2017/TRAVELSHED/travelshedrevamp/location/google_transit_staten_island/'
stoptime=pd.read_csv(path+'stop_times.txt')
trip=pd.read_csv(path+'trips.txt')
stop=pd.read_csv(path+'stops.txt')
df=pd.merge(stoptime,trip,how='outer',on='trip_id')
df=pd.merge(df,stop,how='outer',on='stop_id')
df=df[['stop_id','stop_name','stop_lat','stop_lon','route_id']]
df=df.drop_duplicates(keep='first')
df.to_csv(path+'df.csv',index=False)



path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2017/TRAVELSHED/travelshedrevamp/location/google_transit_busco/'
stoptime=pd.read_csv(path+'stop_times.txt')
trip=pd.read_csv(path+'trips.txt')
stop=pd.read_csv(path+'stops.txt')
df=pd.merge(stoptime,trip,how='outer',on='trip_id')
df=pd.merge(df,stop,how='outer',on='stop_id')
df=df[['stop_id','stop_name','stop_lat','stop_lon','route_id']]
df=df.drop_duplicates(keep='first')
df.to_csv(path+'df.csv',index=False)