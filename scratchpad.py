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


#print(station)
# url='https://wcc.sc.egov.usda.gov/awdbWebService/services?WSDL'
# client = Client(url)
# result=client.service.getHourlyData(stationTriplets='415:CO:SNTL', 
#                                     elementCd='PREC', 
#                                     ordinal=1, 
#                                     beginDate='1978-01-01', 
#                                     endDate='1978-12-31')

# print(result)

def get_stations(url='https://wcc.sc.egov.usda.gov/awdbWebService/services?WSDL', 
                 network_codes = ['SNTL']):

    '''Returns a list of all stations within a network given a list of 
    network codes'''
    client = Client(url)
    result = client.service.getStations(networkCds=network_codes)
    return(result)

stations = get_stations()
print(len(stations))
#print(stations)

