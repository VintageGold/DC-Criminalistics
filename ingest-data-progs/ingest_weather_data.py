"""
This program ingests weather data from Dark Sky API using DC crime data.
"""
import json
import pandas as pd
import requests
import zipfile
import sqlite3

"""
Import crime data and format date/time for API calls.
"""
def formatCrimeData():
    crime_zip = zipfile.ZipFile("../data/dc-crime-data/dc-crime-data.csv.zip", mode='r')
    crime_csv = crime_zip.open('dc-crime-data.csv')
    crime_df = pd.read_csv(crime_csv)

    crime_df['STATE_DATE_TRUNC'] = crime_df['START_DATE'].str[:-4]

    crime_df.dropna(subset=['STATE_DATE_TRUNC','LATITUDE','LONGITUDE'], how='any', inplace=True)

    crime_df_nodup = crime_df.drop_duplicates(['STATE_DATE_TRUNC','LATITUDE','LONGITUDE'])

    return crime_df_nodup.loc[:1]

"""
Make an API call for a particular time/date/location of crime, return as JSON.
"""
def darkSkyAPICall(df_api):
    base_url = 'https://api.darksky.net/forecast/'
    api_key = '15f9fff0f7f470836b00d3e1ceeaff2a'
    exclude = 'minutely, hourly, daily, flags'
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
Write a Dark Sky API return to DB file.
"""
def writeDatabaseFile(json_doc):

    #Turn into Dataframe for easier export.
    weather_df = pd.io.json.json_normalize(json_doc, sep="_")

    #Connect to DB table.
    conn = sqlite3.connect('../data/weather-data/weather_data_test.db')
    c = conn.cursor()

    #Drop table if exists.
    c.execute("drop table if exists weather_data_test")

    weather_df.to_sql('weather_data_test',conn)

    #Commit and close connection.
    conn.commit()
    conn.close()

"""
The driver function.
"""
def main():
    crime_df = formatCrimeData()

    weather_list = []

    for index, row in crime_df.iterrows():
        weather_json = darkSkyAPICall(row)
        weather_list.append(weather_json)

    writeDatabaseFile(json_doc=weather_list)

if __name__ == "__main__":
    main()
