"""
This program ingests weather data from Dark Sky API using DC crime data.
"""
import pandas as pd
import requests
import json

"""
Import crime data and format date/time for API calls.
"""
def formatCrimeData():
    crime_df = pd.read_csv("crime-data/dc-crimes-search-results.csv")

    crime_df['STATE_DATE_TRUNC'] = crime_df['START_DATE'].str[:-4]

    crime_df.dropna(subset=['STATE_DATE_TRUNC','LATITUDE','LONGITUDE'], how='any', inplace=True)

    crime_df_nodup = crime_df.drop_duplicates(['STATE_DATE_TRUNC','LATITUDE','LONGITUDE'])

    return crime_df_nodup

"""
Make an API call for a particular time/date/location of crime, return as JSON.
"""
def darkSkyAPICall(df_api):
    base_url = 'https://api.darksky.net/forecast/'
    api_key = ''
    exclude = 'minutely'
    params = {'exclude': exclude}

    query = "/{},{},{}".format(df_api['LATITUDE'],df_api['LONGITUDE'],df_api['STATE_DATE_TRUNC'])

    url = base_url + api_key + query

    try:
        response = requests.get(url, params=params)
    except ConnectionError:
        pass

    try:
        response_json = response.json()
    except:
        response_json = {}

    return response_json

"""
Write a Dark Sky API return to text (.json) file.
"""
def writeJSON(json_doc):
        json_text = json.dumps(json_doc)

        file_name = "weather-data/weather_data.json"

        with open(file_name, 'wb') as file:
            file.write(json_text.encode('utf-8'))

"""
The driver function.
"""
def main():
    df = formatCrimeData()

    weather_data = []

    for index, row in df.iterrows():
        response = darkSkyAPICall(row)
        weather_data.append(response)

    writeJSON(weather_data)

if __name__ == "__main__":
    main()
