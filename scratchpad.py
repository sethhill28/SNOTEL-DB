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

def get_station_metadata(url='https://wcc.sc.egov.usda.gov/awdbWebService/services?WSDL'):
    '''Returns a DataFrame containing the metadata for all SNOTEL stations'''
    stations = get_snotel_stations()
    dfs = []
    for station in stations:
        result = client.service.getStationMetadata(station)
        data = Client.dict(result)
        # convert each value to a list
        for key in data.keys():
            data[key] = [data[key]]
        df = pd.DataFrame(data)
        dfs.append(df)

    stacked_df = pd.concat(dfs)
    return(stacked_df)

df = get_station_metadata()
print(df)
