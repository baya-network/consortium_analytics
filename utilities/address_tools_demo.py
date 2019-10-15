# Here are handy functions for address cleanup & handling opertions
# TO-DO:x return a match confidence score along with matched RY ID. Option a) use fuzzywuzzy against all addresses of returned id

import re
import usaddress
import requests
import os
import pandas as pd, numpy as np
import json

# list of 500 common address abbreviations published by the United States Postal Service (USPS) https://pe.usps.com/text/pub28/28apc_002.htm
with open(os.path.join(os.path.dirname(__file__),"usps_abbreviations.json"), "r") as read_file:
    usps_abbreviations = json.load(read_file)

# 
usps_abbreviations['n'] = 'North'
usps_abbreviations['e'] = 'East'
usps_abbreviations['s'] = 'South'
usps_abbreviations['w'] = 'West'


def expand_abbv(address, separator=None, index_of_abbv=-1):
    """ 
    Expands typical address string abbreviations. Leverages the str.split() function.

    Parameters
    ----------
    address : str
        String containing the abbreviation
    separator : str, optional
        Separator used to break up the string and isolate the abbreviation, input to str.split(). Defaults to whitespace.
    index_of_abbv : int, optional
        Index of the abbreviated word in the list returned by str.split()

    Returns
    -------
    On success: Original string with expanded abbreviation
    Otherwise: Original string
    """
    # assert ( type(address) is str and len(address) > 1 ), "input is not a address string"
    try:
        # removing whitespace in the beginning and ending
        address = address.strip()
        # splitting on whitespace
        add_parts = address.split(separator)
        # checking if the lowercased, purely alphanumeric, address piece is a known abbreviation, if so expand it
        abbvs = [usps_abbreviations[re.sub('[^\w\s]', '', abbv).lower()]
                 if re.sub('[^\w\s]', '', abbv).lower() in usps_abbreviations.keys() 
                 else abbv 
                 for abbv in add_parts]
        # joining by whitespace the words of the address
        expanded_address = ' '.join(abbvs)
        return expanded_address
    except Exception as e:
        return e, e.args
    else:
        return address + ': is not a valid address'


# To-do: a create abbv function, the contrary of the one above, e.g. from Street to St. 
# Probably just an if statement in the above fn, checking if abbv.lower() in list(usps_abbreviations.values())


street_comps = [
   'AddressNumber',
   'AddressNumberPrefix',
   'AddressNumberSuffix',
   'StreetName',
   'StreetNamePreDirectional',
   'StreetNamePreModifier',
   'StreetNamePreType',
   'StreetNamePostDirectional',
   'StreetNamePostModifier',
   'StreetNamePostType',
   'LandmarkName'
]

city_comps = ['PlaceName']
state_comps = ['StateName']
zip_comps = ['ZipCode']


def parse_address(address):
    """ 
    Parses address strings and categorizes into conveninent components for Baya common functions.

    Parameters
    ----------
    address : str
        Address string typical formed by a street number, street name, city, zipcode.

    Returns
    -------
    On success: a dictionary of 4 possible keys: address (street # + street name), city, state, and zipcode.  
    Otherwise: original string
    """
    assert ( type(address) is str and len(address) > 1 ), "input is not a address string"
    try:
        ord_dict = usaddress.tag(address)[0]
        if len(ord_dict.keys()) > 0:
            res = {}
            street_values = [ord_dict[key] for key in ord_dict if key in street_comps]
            print(' '.join(street_values))
            if len(street_values) > 0:
                res['address'] = expand_abbv(' '.join(street_values))
            if 'PlaceName' in ord_dict.keys():
                res['city'] = ord_dict['PlaceName'] 
            if 'StateName' in ord_dict.keys():
                res['state'] = ord_dict['StateName']
            if 'ZipCode' in ord_dict.keys():
                res['zipcode'] = ord_dict['ZipCode']
            if res:
                return res
            else:
                res['address'] = address
                return res
        else: 
            return address + ': is not a valid address'
    except Exception as e:
        return e, e.args


def make_req_obj_from_dict(d, idx=None):
    """ 
    Takes a parsed address dictionary and creates the reonomy-formatted object the get ry_id call

    Parameters
    ----------
    d : dictionary
        Takes a dictionary of the components of a typical address (street name, city, zipcode, state). The output from parse_address
    idx : index
        An index value to serve as an identifier to map the a particular request to the response, particularly useful in batch calls. 

    Returns
    -------
    On success: the required object format to call Reonomy's match endpoint
    Otherwise: raises exceptions
    """
    assert ('address' in d.keys()), "address has no street name nor street number"
    try:
        obj = {}
        obj['line1'] = d['address']
        obj['city'] = d['city'] if 'city' in d.keys() else 'New York'
        obj['state'] = "NY"                          # For now only NY is supported
        if 'zipcode' in d.keys():
            obj['postal_code'] = d['zipcode']

        req_obj = { 'addresses': [], 'custom_id': ''}
        req_obj['addresses'].append(obj)
        if idx is not None:
            req_obj['custom_id'] = str(idx)
        return req_obj
    
    except Exception as e:
        return e, e.args


match_endpoint = "https://api.reonomy.com/v1/nyc/properties/matches"
credentials = ('baya', os.environ["RY_API_KEY"])

def get_ry_id_for_address(address):
    """ 
    Calls RY match endpoint to retrieve the reonomny ID of the inputted address. Leverages functions above

    Parameters
    ----------
    address: string
        input address string of the building to be matched
    Returns
    -------
    On success: the reonomy_id for the building at the inputted address
    Otherwise: "no matching building found"
    """
    try:
        parsed_address = parse_address(address)
        req_obj = make_req_obj_from_dict(parsed_address)
        params = {'params': [req_obj]}
        r = requests.post(match_endpoint, auth=credentials, json=params)
        if 'property_id' in r.json()['matches'][0].keys():
            return r.json()['matches'][0]['property_id']
        else: 
            return "No matching building found"
    except Exception as e:
        return e, e.args


def get_ry_id_for_df(dataframe, address_col_name='address'):
    """ 
    Calls RY match endpoint in batches of 100 addresses, returns a Series of reonomny ID in the order of the received addresses.

    Parameters
    ----------
    dataframe: Pandas DataFrame
        a dataframe containing a column, or various, with the addresses fo the bldgs to be matched to RY ids. 
    address_col_name: string
        name of the column containing the addresses, defaults to 'address'.
    Returns
    -------
    On success: A Pandas Serie with the Reonomy ID of the inputted addresses, in order. In case of no match, NaN
    -------
    TO-DO: add support for address to be separated in various columns e.g. city_col, zipcode_col, instead of all the address string in a single column.
    """
    results = pd.DataFrame(columns=['params', 'property_id'])
    for i in np.arange(0, dataframe.shape[0], 100):
        try:
            # getting and parsing 100 addresses from the DF to batch RY endpoint
            address_df = pd.DataFrame(dataframe.loc[:, address_col_name].iloc[i :i+100].apply(parse_address))
            address_df.index.name = 'original_idx'
            address_df.reset_index(inplace=True)

            # making req objects
            req_obj_serie = address_df.apply(lambda x: make_req_obj_from_dict(x[address_col_name], x['original_idx']), axis=1)
            # grouping req objects for batch call
            great_params = {"params": req_obj_serie.tolist()}
            r = requests.post(match_endpoint, auth=credentials, json=great_params)
            # stacking each item of result object
            results = results.append(pd.DataFrame.from_dict(r.json()['matches']), sort=True, ignore_index=False)
            # getting the original id for each row
            results.set_index(results.params.apply(lambda x: int(x['custom_id'])), inplace=True)
            results.index.name = 'original_idx'
            print("The {}'s are running".format(i))
        except Exception as e:
            results = results.append({'params': str(e)+ ' ' + str(e.args), 'property_id': np.nan}, ignore_index=False)
            print("error on the {}'s".format(i)) #'property_id': "ERROR on match call b/w rows {} & {}".format(100*i,100*i+100)}
    return results.property_id



get_multiple_endpoint = "https://api.reonomy.com/v1/nyc/properties"
body = {
  "property_ids": [],
  "include_contacts": False,
  "include_financials": False,
  "include_rent_regulated": False
}


def get_all_addresses_for_ry_id(dataframe, ry_id_col_name='ry_id'):
    """
    Calls RY's Get Multiple endpoint in batches of 100 ry_ids; gets and returns all addresses associated with each ry_id.
    Takes ~1min for every 300 ry_ids. 
    
    Parameters
    ----------
    dataframe: Pandas DataFrame
        a dataframe containing a column, with the ry_ids.
    address_col_name: string
        name of the column containing the ry_ids, defaults to 'ry_id'.
    Returns
    -------
    On success: A Pandas DataFrame with the Reonomy ID inputted, and a list of addresses associated with each
    """
    results = pd.DataFrame(columns=['ry_id', 'addresses'])
    for i in np.arange(0, dataframe.shape[0], 100):
        try:
            body["property_ids"] = dataframe.iloc[i:i+100].loc[:, ry_id_col_name].tolist()
            r = requests.post(get_multiple_endpoint, auth=credentials, json=body)   
            sin = [{"ry_id": prop['id'], 
                    "addresses": [dic['line1'] for dic in prop['addresses']] } for prop in r.json()['properties']]
            results = results.append(pd.DataFrame(sin), sort=True)
            print("The {}'s are running".format(i))
        except Exception as e:
            results = results.append({'params': str(e)+ ' ' + str(e.args), 'property_id': np.nan}, ignore_index=True)
            print("error on the {}'s".format(i))
    return results