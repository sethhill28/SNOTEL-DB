from suds.client import Client
import xmltodict
import pandas as pd

url = 'https://wcc.sc.egov.usda.gov/awdbWebService/services?WSDL'
client = Client(url)

result = client.service.getStations()
#print(result)

def get_snotel_stations(url='https://wcc.sc.egov.usda.gov/awdbWebService/services?WSDL'):
    '''Returns a list of all SNOTEL stations'''
    client = Client(url)
    result = client.service.getStations()
    return(result)

stations = get_snotel_stations()

station = stations[0]

result = client.service.getStationMetadata(station)
print(result)