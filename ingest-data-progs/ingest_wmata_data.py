"""
This program ingests WMATA bus stop locations and rail entrances.
"""
import json
import pandas as pd
import requests
from pprint import pprint

"""
Make an API call for a particular time/date/location of crime, return as JSON.
"""
def wmataAPICall(method):
    if method.upper() == 'BUS':
        url = 'https://api.wmata.com/Bus.svc/json/jStops'
    elif method.upper() == 'RAIL':
        url = 'https://api.wmata.com/Rail.svc/json/jStationEntrances'
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
Write a Dark Sky API return to text (.json) file.
"""
def writeJSON(json_doc, method):
        json_text = json.dumps(json_doc)

        file_name = "wmata-data/{}-data.json".format(method)

        with open(file_name, 'wb') as file:
            file.write(json_text.encode('utf-8'))

"""
The driver function.
"""
def main(method):
    wmata_data = wmataAPICall(method=method)

    writeJSON(json_doc=wmata_data, method=method)

if __name__ == "__main__":
    main('rail')
    main('bus')
