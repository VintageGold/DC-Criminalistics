"""
This program ingests DC Crime Data, available from 2008-2019.
"""
import json
import pandas as pd
import requests
from pprint import pprint

"""
Make an API call for a specific year of DC crime data from DC Open Data, return as JSON.
"""
def DCCrimeDataAPICall(method):

    if str(method) == '2008':
        url = 'https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/MPD/MapServer/32/query?where=1%3D1&outFields=*&outSR=4326&f=json'
    elif str(method) == '2009':
        url = 'https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/MPD/MapServer/33/query?where=1%3D1&outFields=*&outSR=4326&f=json'
    elif str(method) == '2010':
        url = 'https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/MPD/MapServer/34/query?where=1%3D1&outFields=*&outSR=4326&f=json'
    elif str(method) == '2011':
        url = 'https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/MPD/MapServer/35/query?where=1%3D1&outFields=*&outSR=4326&f=json' 
    elif str(method) == '2012':
        url = 'https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/MPD/MapServer/11/query?where=1%3D1&outFields=*&outSR=4326&f=json'
    elif str(method) == '2013':
        url = 'https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/MPD/MapServer/10/query?where=1%3D1&outFields=*&outSR=4326&f=json'
    elif str(method) == '2014':
        url = 'https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/MPD/MapServer/9/query?where=1%3D1&outFields=*&outSR=4326&f=json' 
    elif str(method) == '2015':
        url = 'https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/MPD/MapServer/27/query?where=1%3D1&outFields=*&outSR=4326&f=json'         
    elif str(method) == '2016':
        url = 'https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/MPD/MapServer/26/query?where=1%3D1&outFields=*&outSR=4326&f=json'
    elif str(method) == '2017':
        url = 'https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/MPD/MapServer/38/query?where=1%3D1&outFields=*&outSR=4326&f=json'
    elif str(method) == '2018':
        url = 'https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/MPD/MapServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json'        
    elif str(method) == '2019':
        url = 'https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/MPD/MapServer/1/query?where=1%3D1&outFields=*&outSR=4326&f=json'    

    else: 
        print("Method not recognized.")

    api_key = ''
    headers = {'api_key': api_key}

    try:
        response = requests.get(url, headers=headers)
    except:
        print("Could not make request.")

    try:
        response_json = response.json()
    except:
        response_json = {}

    return response_json

"""
Write a DC Open Data call to text (.json) file.
"""
def writeJSON(json_doc, method):
        json_text = json.dumps(json_doc)

        file_name = "dc-crime-data/{}-crime-data.json".format(method)
        with open(file_name, 'wb') as file:
            file.write(json_text.encode('utf-8'))

"""
The driver function.
"""
def main(method):
    dc_crime_data = DCCrimeDataAPICall(method=method)

    writeJSON(json_doc=dc_crime_data, method=method)

if __name__ == "__main__":
    main('2008')
    main('2009')
    main('2010')
    main('2011')
    main('2012')
    main('2013')
    main('2014')
    main('2015')
    main('2016')
    main('2017')
    main('2018')
    main('2019')