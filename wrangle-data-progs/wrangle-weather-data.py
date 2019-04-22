'''
Import Libraries
'''
import zipfile
import pandas as pd
import csv
import json
import datetime

'''
Import Weather Data
'''
def importWeatherData():
    #Import the weather data as a ZIP file.
    weather_zip = zipfile.ZipFile("../weather-data/crime-weather-data.zip", mode='r')
    weather_json = weather_zip.open('crime-weather-data.json')
    weather_dict = json.load(weather_json)

    #Return as a dictionary, converted from JSON.
    return weather_dict

'''
Wrangle Weather Data
'''
def convertTime(row):
    time = datetime.datetime.fromtimestamp(row).strftime('%Y-%m-%dT%H:%M:%S.000')

    return time

def wrangleWeatherData(weather_dict):
    #Move the items contained in "currently" key up one level.
    for w_dict in weather_dict:
        for w_entry in list(w_dict.items()):
            if w_entry[0] == 'currently':
                for items in w_entry[1].items():
                    w_dict[items[0]] = items[1]

                del w_dict['currently']

    #Create Pandas DataFrame from weather dictionary.
    weather_df = pd.DataFrame(weather_dict)

    #There are negative time values.
    weather_df = weather_df[weather_df['time'] > 0]

    #Convert time to format shared by crime data.
    weather_df['c_time'] = weather_df['time'].apply(convertTime)

    return weather_df
