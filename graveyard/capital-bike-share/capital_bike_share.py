import json
import requests
import pandas as pd
from pandas.io.json import json_normalize

url = 'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Transportation_WebMercator/MapServer/5/query?where=1%3D1&outFields=*&outSR=4326&f=json'

response = requests.get(url)
d = response.json()

df = json_normalize(d['features'])
