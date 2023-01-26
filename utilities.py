import pandas as pd
import time
from numpy import nan
import requests
import xmltodict
from math import isnan
from geopy import distance
from suds.client import Client
import config

def get_df(method_name, 
           url = 'https://wcc.sc.egov.usda.gov/awdbWebService/services?WSDL'):
    '''Creates dataframe for the getElements method or getUnits method used for 
    creating data dictionary'''
    
    client = Client(url)
    if method_name == 'getElements':
        result = client.service.getElements()
    elif method_name == 'getUnits':
        result = client.service.getUnits()
    else:
        print("Unaccepted method name, please enter 'getElements' or 'getUnits'")
        return
    data = [Client.dict(suds_object) for suds_object in result]
    df = pd.DataFrame(data)
    return(df)

def create_data_dictionary(url = 'https://wcc.sc.egov.usda.gov/awdbWebService/services?WSDL'):

    '''Creates a data dictionary by pulling variable info'''
    
    elements = get_df(method_name = 'getElements')
    units = get_df(method_name='getUnits')

    df = pd.merge(elements, units, left_on='storedUnitCd', right_on = 'unitCd')
    df.drop(columns=['storedUnitCd', 'unitCd'], inplace=True)
    mapper = {'elementCd': 'elementCd', 
            'name_x': 'element_name', 
            'name_y': 'units'}
    df.rename(columns=mapper, inplace=True)
    return(df)

def get_snotel_stations(url='https://wcc.sc.egov.usda.gov/awdbWebService/services?WSDL'):
    '''Returns a list of all SNOTEL stations'''
    client = Client(url)
    result = client.service.getStations()
    return(result)

# def get_station_locations(url = 'https://hydroportal.cuahsi.org/Snotel/cuahsi_1_1.asmx?WSDL', 
#                           return_missing_locations = False):
#     '''pulls location of every SNOTEL site when available'''
#     codes = []
#     names = []
#     networks = []
#     latitudes = []
#     longitudes = []
#     missing_sites = []
#     df = pd.DataFrame()

#     response = ulmo.cuahsi.wof.get_sites(url)
#     sites = list(response.keys())
#     start = time.time()

#     for site in sites:
#         site_info = ulmo.cuahsi.wof.get_site_info(url, site)
#         code_test = 'code' in site_info.keys()
#         name_test = 'name' in site_info.keys()
#         network_test = 'network' in site_info.keys()
#         location_test = 'location' in site_info.keys()
#         if location_test:
#             latitude_test = 'latitude' in site_info['location'].keys()
#             longitude_test = 'longitude' in site_info['location'].keys()
        
#         if (code_test and name_test and network_test and location_test 
#             and latitude_test and longitude_test):
#             codes.append(site_info['code'])
#             names.append(site_info['name'])
#             networks.append(site_info['network'])
#             latitudes.append(site_info['location']['latitude'])
#             longitudes.append(site_info['location']['longitude'])
#         else:
#             missing_sites.append(site)
#             codes.append(nan)
#             names.append(nan)
#             networks.append(nan)
#             latitudes.append(nan)
#             longitudes.append(nan)

#     df['code'] = codes
#     df['name'] = names
#     df['network'] = networks
#     df['latitiude'] = latitudes
#     df['longitude'] = longitudes
#     end = time.time()
#     print('Pulled all available locations in', round(end - start, 4), 'seconds')
#     print(f'Location unavaialble for the following {len(missing_sites)} stations: ')
#     for station in missing_sites:
#         print(station)
#     df.dropna(inplace=True)
#     result =  [df, ]
#     if return_missing_locations:
#         result.append(missing_sites)
#     return result

def get_ski_area_info(url='https://skimap.org/SkiAreas/index.xml', 
                      return_missing_geo_info = False, 
                      return_missing_region_info = False):
    '''returns a dataframe containing ski area information and any ski areas 
       whose geographic info is missing when return_missing_geo_info = True'''

    response = requests.get(url)
    d = xmltodict.parse(response.text)
    df = pd.read_xml(response.text)

    lats = []
    lngs = []
    regions = []
    locations = d['skiAreas']['skiArea']
    missing_geo_info = []
    missing_region_info = []
    # custom map for ski areas in multiple regions
    custom_map = {'Arlberg (St Anton, St Christoph, Stuben, Lech, ​Zürs, Warth, ​Schröcken)': '346', 
                'Silvretta Arena (Ischgl/Samnaun)': '344', 
                'Matterhorn (Zermatt/​Breuil-Cervinia/​Valtournenche)': '387', 
                'Les Portes du Soleil (Morzine, Avoriaz, ​Les Gets, ​Châtel, ​Morgins, ​Champéry)': '381'}
    for location in locations:
        # get lat and lng
        geo_ref_test = 'georeferencing' in location.keys()
        if geo_ref_test:
            lat_test = '@lat' in location['georeferencing'].keys()
            lng_test = '@lng' in location['georeferencing'].keys()
        if (geo_ref_test and lat_test and lng_test):
            lats.append(location['georeferencing']['@lat'])
            lngs.append(location['georeferencing']['@lng'])
        else:
            missing_geo_info.append(location['name'])
            lats.append(nan)
            lngs.append(nan)
            
        # get regional info
        name = location['name']
        if location['regions']:
            if name in custom_map.keys():
                regions.append(custom_map[name])
            else:
                regions.append(location['regions']['region']['@id'])
        else:
            missing_region_info.append(name)
            regions.append(nan)
    
    df['latitude'] = lats
    df['longitude'] = lngs
    df['region'] = regions
     # drop non-needed columns
    df.drop(columns=['georeferencing', 'regions'], inplace=True)
    
    result = [df, ]
    if return_missing_geo_info:
        result.append(missing_geo_info)
    if return_missing_region_info:
        result.append(missing_region_info)
    return result

def get_geo_info(lat, lng):
    '''Returns the country, administrative level 1 (state in the US) 
       and administrative level 2 (county in the US) for a given latitude and 
       longitude'''
    url = 'https://maps.googleapis.com/maps/api/geocode/json'
    params = {'latlng': f"{lat},{lng}", 
            'key': api_key,
            'result_type': 'country|administrative_area_level_1|administrative_area_level_2'
    }

    response = requests.get(url, params=params)
    data = response.json()['results']
    d = {}
    for obj in data:
        names = []
        type = obj['types'][0]
        name = obj['address_components'][0]['long_name']
        names.append(name)
        d[type] = names

    df = pd.DataFrame(d)
    return df 

def get_ski_area_geo_info_hist():
    '''Returns geo info for all ski areas'''
    ski_areas = get_ski_area_info()[0]

    lats = ski_areas['latitude'].tolist()
    lngs = ski_areas['longitude'].tolist()
    d = dict(zip(lats, lngs))

    counter = 0
    dfs = []
    for lat in d.keys():
        lng = d[lat]
        if (isnan(float(lat))  or isnan(float(lng))):
            continue
        else:
            df = get_geo_info(lat, lng)
            dfs.append(df)
        counter += 1
        print(round(counter / len(d.keys()) * 100, 2), "percent done")
    stacked_df = pd.concat(dfs)
    return stacked_df

def n_closest(data, ref_point, n, units):
    '''Returns the n closest points from a list of tuples containing
    latitude and longitudes (data) to a reference point (ref_point)'''
    distances = []
    for point in data:
        if units == 'miles':
            calc_distance = distance.distance(ref_point, point).miles
        elif units == 'km':
            calc_distance = distance.distance(ref_point, point).km
        else:
            print(f"'{units}' is not an accepted argument, please choose between 'miles' or 'km'")
            return
        distances.append(calc_distance)
    return sorted(distances)[:n]

df = create_data_dictionary()
print(df)