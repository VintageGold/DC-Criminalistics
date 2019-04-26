################################################################################
## Imports
################################################################################
import zipfile
import pandas as pd
import csv
import json
import datetime
import sqlite3

################################################################################
## Import Data
################################################################################
def importCensusData():
    #Import directly from CSV and return a DataFrame.
    census_df = pd.read_csv('../census-data/FinalBlockGroupData.csv')

    return census_df

def importWeatherData():
    #Import the weather data by unpacking ZIP, JSON, return a DataFrame.
    weather_zip = zipfile.ZipFile("../weather-data/crime-weather-data.zip", mode='r')
    weather_json = weather_zip.open('crime-weather-data.json')
    weather_dict = json.load(weather_json)

    #Return as a dictionary, converted from JSON.
    return weather_dict

def importCrimeData():
    #Import the crime data by unpacking the ZIP file, converting to CSV, then returning DataFrame.
    crime_zip = zipfile.ZipFile("../dc-crime-data/dc-crime-data.csv.zip", mode='r')
    crime_csv = crime_zip.open('dc-crime-data.csv')
    crime_df = pd.read_csv(crime_csv)

    return crime_df

################################################################################
## Wrangle Data
################################################################################
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

def wrangleCensusData(census_df):
    #Fill NA values and values less than 0 with the mean of values greater than zero.
    columns = ['TotalPop','TPopMargin','UnWgtSampleCtPop','PerCapitaIncome','PerCapIncMargin','MedianHouseholdInc',
    'MedHouseholdIncMargin','MedianAge','MedianAgeMargin','HousingUnits','HousingUnitsMargin',
    'UnweightedSampleHousingUnits']

    for col in columns:
        #Calculate Mean
        mean = census_df[census_df[col] > 0][col].mean()

        #Fill NA with a dictionary of column name and the mean value
        census_df.fillna(value={col: mean}, inplace=True)

        #Replace values less than zero with the mean.
        census_df[census_df[col] < 0] = mean

    #I left this as is.
    census_df['BlockGroup'] = census_df['BlockGroup'].astype(str).replace(']]', '', regex=True)
    census_df['Tract'] = census_df['Tract'].astype(str).replace('\.0', '', regex=True)
    census_df['Tract'] = census_df['Tract'].apply(lambda x: x.zfill(6))
    census_df['BlockGroup'] = census_df['BlockGroup'].astype(str).replace('\.0', '', regex=True)
    census_df['Year'] = census_df['Year'].astype(str).replace('\.0', '', regex=True)

    #I moved this to the wrangle function.
    census_df['index'] = census_df['Tract'] + " " + census_df['BlockGroup'] + " " + census_df['Year']
    census_df_nodup = census_df.drop_duplicates(subset='index')

    #Instead of dropping by deleting Key, I used Drop function from Pandas.
    census_df_nodup.drop(labels=['Unnamed: 0'], axis='columns', inplace=True)

    return census_df_nodup

def wrangleCrimeData(crime_df):
    crime_df['YEAR'] = crime_df['YEAR'].astype(str)
    crime_df['BLOCK_GROUP'] = crime_df['BLOCK_GROUP'].astype(str)
    crime_df['index'] = crime_df['BLOCK_GROUP'] + " " + crime_df['YEAR']

    return crime_df

def createdb(data):
    #data = data.loc[:,~data.columns.duplicated()]
    #del data['Year']
    #conn = sqlite3.connect('crime_census_weather.db')
    #data.to_sql('crime_census_weather', conn)
    pass

################################################################################
## Driver
################################################################################
def main():
    crime_df = importCrimeData()
    census_df = importCensusData()
    weather_dict = importWeatherData()

    weather_df_wr = wrangleWeatherData(weather_dict)
    census_df_wr = wrangleCensusData(census_df)
    crime_df_wr = wrangleCrimeData(crime_df)

    crime_census_mr = crime_df_wr.merge(census_df_wr.drop_duplicates(subset=['index']), how='left', on='index', indicator=True)

    crime_census_weather_mr = crime_census_mr.merge(weather_df_wr, how='left', left_on=['LATITUDE','LONGITUDE','START_DATE'], right_on=['latitude','longitude','c_time'])
    print(crime_census_weather_mr.columns)

    createdb(crime_census_weather_mr)

if __name__ == '__main__':
    main()
