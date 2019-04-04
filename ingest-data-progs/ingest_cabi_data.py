'''
Import Libraries
'''
import json
import requests
import pandas as pd

'''
Pull CABI station locations from https://gbfs.capitalbikeshare.com/gbfs/en/station_information.json
'''
def cabiWebPull():
    url = 'https://gbfs.capitalbikeshare.com/gbfs/en/station_information.json'

    response = requests.get(url)
    data = response.json()

    return data

'''
Write the station information to a JSON file in the folder.
'''
def writeJSON(json_doc):
    json_text = json.dumps(json_doc)

    file_name = "../cabi-station-data/cabi-station-data.json"

    with open(file_name, 'wb') as file:
        file.write(json_text.encode('utf-8'))

"""
The driver function.
"""
def main():
    cabi_locations = cabiWebPull()
    writeJSON(cabi_locations)

if __name__ == "__main__":
    main()
