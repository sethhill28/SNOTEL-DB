import pandas as pd
import time
from numpy import nan
import requests
import xmltodict
from math import isnan
from geopy import distance
from suds.client import Client
import config
from utilities import *

# url = 'https://wcc.sc.egov.usda.gov/awdbWebService/services?WSDL'
# client = Client(url)
# stations = get_snotel_stations()
# result = client.service.getStationMetadataMultiple(stations)
# dfs = []
# for station in result:
#     data = Client.dict(station)
#     for key in data.keys():
#         data[key] = [data[key]]
#     df = pd.DataFrame(data)
#     dfs.append(df)
# final_df = pd.concat(dfs)
# final_df.to_csv('station-meta.csv')
# print(final_df)

def get_station_metadata(url='https://wcc.sc.egov.usda.gov/awdbWebService/services?WSDL',
                        stations = get_snotel_stations()):
    '''Returns a DataFrame containing the metadata for a given list of SNOTEL 
    stations (defaults to all)'''
    client = Client(url)
    result = client.service.getStationMetadataMultiple(stations)
    dfs = []
    for station in result:
        data = Client.dict(station)
        for key in data.keys():
             # convert each value to a list
            data[key] = [data[key]]
        df = pd.DataFrame(data)
        dfs.append(df)
    stacked_df = pd.concat(dfs)
    return(stacked_df)


df = get_station_metadata()
print(df)
#print(result[0])
#print(type(result[0]))
#stations = get_station_metadata()
#stations.to_csv('station-meta-data.csv')
#print(station)

# result=client.service.getHourlyData(stationTriplets='302:OR:SNTL', 
#                                     elementCd='PREC', 
#                                     ordinal=1, 
#                                     beginDate='2010-01-01', 
#                                     endDate='2010-01-31')

#print(result)

#result = client.service.getStationMetadataMultiple

