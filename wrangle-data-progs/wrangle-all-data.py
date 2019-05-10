################################################################################
## Imports
################################################################################
import pandas as pd
import datetime
import sqlite3
import zipfile

################################################################################
## Import Data
################################################################################
def importCensusData():
    '''
    Import from DB table and return a DataFrame.
    '''
    conn = sqlite3.connect('../data/census-data/census_bg.db')
    census_df = pd.read_sql('''select * from census_blockgroup;''', conn)
    conn.close()

    return census_df

def importWeatherData():
    '''
    Import the weather data by accessing DB table.
    '''
    conn = sqlite3.connect('../data/weather-data/weather_data.db')
    weather_df = pd.read_sql('''select * from weather_data;''', conn)
    conn.close()

    return weather_df

def importCrimeData():
    '''
    Import the crime data by unpacking the ZIP file, converting to CSV, then returning DataFrame.
    '''
    crime_zip = zipfile.ZipFile("../data/dc-crime-data/dc-crime-data.csv.zip", mode='r')
    crime_csv = crime_zip.open('dc-crime-data.csv')
    crime_df = pd.read_csv(crime_csv)

    return crime_df

################################################################################
## Wrangle Data
################################################################################
def convertTime(row):
    time = datetime.datetime.fromtimestamp(row).strftime('%Y-%m-%dT%H:%M:%S.000')

    return time

def wrangleWeatherData(weather_df):
    #There are negative time values that must be removed.
    weather_df = weather_df[weather_df['currently_time'] > 0]

    #Convert time to format shared by crime data.
    weather_df['crime_time'] = weather_df['currently_time'].apply(convertTime)

    #Rename and drop columns.
    rename = dict(currently_apparentTemperature = 'apparent_temp',
    currently_cloudCover = 'cloud_cover',
    currently_dewPoint = 'dew_point',
    currently_humidity = 'humidity',
    currently_icon = 'icon',
    currently_precipIntensity = 'percip_intensity',
    currently_precipProbability = 'percip_probability',
    currently_precipType = 'percip_type',
    urrently_pressure = 'pressure',
    currently_summary = 'summary',
    currently_temperature = 'temperature',
    currently_time = 'time',
    currently_uvIndex = 'uv_index',
    currently_visibility = 'visibility',
    currently_windBearing = 'wind_bearing',
    currently_windGust = 'wind_gust',
    currently_windSpeed = 'wind_speed',
    latitude = 'weather_latitude',
    longitude = 'weather_longitude')

    weather_df.rename(columns=rename, inplace=True)
    weather_df.drop(labels=['index','code'], axis='columns', inplace=True)

    return weather_df

def wrangleCensusData(census_df):
    #Fill NA values and values less than 0 with the mean of values greater than zero.
    columns = ['TotalPop','TPopMargin','UnWgtSampleCtPop','PerCapitaIncome','PerCapIncMargin','MedianHouseholdInc',
    'MedHouseholdIncMargin','MedianAge','MedianAgeMargin','HousingUnits','HousingUnitsMargin',
    'UnweightedSampleHousingUnits']

    for col in columns:
        #Edit string (object) columns to numeric (float).
        if census_df[col].dtypes == 'object':
            numeric_column = pd.to_numeric(census_df[col], errors = 'coerce')
            census_df[col] =  numeric_column

        #Calculate Mean.
        mean = census_df[census_df[col] > 0][col].mean()

        #Fill NA with a dictionary of column name and the mean value
        census_df.fillna(value={col: mean}, inplace=True)

        #Replace values less than zero with the mean.
        census_df[census_df[col] < 0] = mean

    #Reformat columns and rename year column.
    census_df['BlockGroup'] = census_df['BlockGroup'].astype(str).replace(']]', '', regex=True)
    census_df['BlockGroup'] = census_df['BlockGroup'].astype(str).replace('\.0', '', regex=True)
    census_df['Tract'] = census_df['Tract'].astype(str).replace('\.0', '', regex=True)
    census_df['Tract'] = census_df['Tract'].apply(lambda x: x.zfill(6))
    census_df['Year'] = census_df['Year'].astype(str).replace('\.0', '', regex=True)
    census_df.rename(columns=dict(Year = 'Census_Year'), inplace=True)

    #Create an index to merge with crime data.
    census_df['index'] = census_df['Tract'] + " " + census_df['BlockGroup'] + " " + census_df['Census_Year']
    census_df_nodup = census_df.drop_duplicates(subset='index')

    return census_df_nodup

def wrangleCrimeData(crime_df):
    crime_df['YEAR'] = crime_df['YEAR'].astype(str)
    crime_df['BLOCK_GROUP'] = crime_df['BLOCK_GROUP'].astype(str)
    crime_df['index'] = crime_df['BLOCK_GROUP'] + " " + crime_df['YEAR']

    return crime_df

def createDB(data):
    #Open connection and create cursor.
    conn = sqlite3.connect('../data/crime_census_weather.db')
    c = conn.cursor()

    #If table already exists, drop it before writing to it.
    c.execute("drop table if exists crime_census_weather")

    #Write to the table.
    data.to_sql('crime_census_weather', conn)

    #Commit and close connection.
    conn.commit()
    conn.close()

#############################################################################
## Driver
#############################################################################
def main():
    crime_df = importCrimeData()
    census_df = importCensusData()
    weather_df = importWeatherData()

    weather_df_wr = wrangleWeatherData(weather_df)
    census_df_wr = wrangleCensusData(census_df)
    crime_df_wr = wrangleCrimeData(crime_df)

    crime_census_mr = crime_df_wr.merge(census_df_wr, how='left', on='index', indicator=True)
    crime_census_weather_mr = crime_census_mr.merge(weather_df_wr, how='left', left_on=['LATITUDE','LONGITUDE','START_DATE'], right_on=['weather_latitude','weather_longitude','crime_time'])

    createDB(crime_census_weather_mr)

if __name__ == '__main__':
    main()
