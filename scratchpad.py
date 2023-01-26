from suds.client import Client
import xmltodict
import pandas as pd

#url = 'https://hydroportal.cuahsi.org/Snotel/cuahsi_1_1.asmx?WSDL'


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

df = create_data_dictionary()

df.to_csv('data-dictionary.csv')


