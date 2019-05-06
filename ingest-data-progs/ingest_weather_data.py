"""
This program ingests weather data from Dark Sky API using DC crime data.
"""
import json
import pandas as pd
import requests
import sqlite3

def formatCrimeData():
    """
    Import crime data and format date/time of crime for Dark Sky API calls.
    """
    crime_zip = zipfile.ZipFile("../data/dc-crime-data/dc-crime-data.csv.zip", mode='r')
    crime_csv = crime_zip.open('dc-crime-data.csv')
    crime_df = pd.read_csv(crime_csv)

    crime_df['STATE_DATE_TRUNC'] = crime_df['START_DATE'].str[:-4]

    crime_df.dropna(subset=['STATE_DATE_TRUNC','LATITUDE','LONGITUDE'], how='any', inplace=True)

    crime_df_nodup = crime_df.drop_duplicates(['STATE_DATE_TRUNC','LATITUDE','LONGITUDE'])

    return crime_df_nodup

def darkSkyAPICall(df_api):
    """
    Make an API call for a particular time/date/location of crime, return as JSON.
    """
    base_url = 'https://api.darksky.net/forecast/'
    api_key = ''
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

def writeDatabaseFile(json_doc):
    """
    Write the return from calling the Dark Sky API to a database file.
    """
    #Create Directory Path
    path_text = '../data/weather-data'
    path = os.path.dirname(path_text)

    #If path does not exist, then make it.
    if not os.path.exists(path):
        os.makedirs(path)

    #Connect to DB table in the folder path.
    conn = sqlite3.connect('../data/weather-data/weather_data.db')
    c = conn.cursor()

    #If table already exists, overwrite it.
    c.execute("drop table if exists weather_data")

    #Turn query into a DataFrame from JSON, flattening JSON file.
    weather_df = pd.io.json.json_normalize(json_doc, sep="_")

    weather_df.to_sql('weather_data',conn)

    #Commit and close connection.
    conn.commit()
    conn.close()

def main():
    """
    The driver function.
    """
    crime_df = formatCrimeData()

    weather_list = []

    for index, row in crime_df.iterrows():
        weather_json = darkSkyAPICall(row)
        weather_list.append(weather_json)

    writeDatabaseFile(json_doc=weather_list)

if __name__ == "__main__":
    main()
