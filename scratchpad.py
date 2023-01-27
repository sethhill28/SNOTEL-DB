import pandas as pd
import time
from numpy import nan
import requests
import xmltodict
from math import isnan
from geopy import distance
from suds.client import Client
import config

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
    #regions = []
    locations = d['skiAreas']['skiArea']
    missing_geo_info = []
    #missing_region_info = []
    # custom map for ski areas in multiple regions
    # custom_map = {'Arlberg (St Anton, St Christoph, Stuben, Lech, ​Zürs, Warth, ​Schröcken)': '346', 
    #             'Silvretta Arena (Ischgl/Samnaun)': '344', 
    #             'Matterhorn (Zermatt/​Breuil-Cervinia/​Valtournenche)': '387', 
    #             'Les Portes du Soleil (Morzine, Avoriaz, ​Les Gets, ​Châtel, ​Morgins, ​Champéry)': '381'}
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
        # name = location['name']
        # if location['regions']:
        #     if name in custom_map.keys():
        #         regions.append(custom_map[name])
        #     else:
        #         regions.append(location['regions']['region']['@id'])
        # else:
        #     missing_region_info.append(name)
        #     regions.append(nan)
    
    df['latitude'] = lats
    df['longitude'] = lngs
    #df['region'] = regions
     # drop non-needed columns
    df.drop(columns=['georeferencing', 'regions', 'id'], inplace=True)
    
    result = [df, ]
    if return_missing_geo_info:
        result.append(missing_geo_info)
    # if return_missing_region_info:
    #     result.append(missing_region_info)
    return result

df = get_ski_area_info()[0]
#df.to_csv('ski_are_info.csv')
print(df)