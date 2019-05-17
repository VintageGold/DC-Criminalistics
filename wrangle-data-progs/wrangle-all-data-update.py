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

def assign_tod(row):
    try:
        timestamp = pd.Timestamp(row['START_DATE'])

        year = timestamp.year
        month = timestamp.month

        start_hour = timestamp.time().hour

        if 0 <= start_hour < 3:
            time_of_day = 'Midnight'
        elif 3 <= start_hour < 6:
            time_of_day = 'Early Morning'
        elif 6 <= start_hour < 12:
            time_of_day = 'Morning'
        elif 12 <= start_hour < 18:
            time_of_day = 'Afternoon'
        elif 18 <= start_hour < 21:
            time_of_day = 'Evening'
        elif 21 <= start_hour <= 23:
            time_of_day = 'Night'
    except:
        year, month, time_of_day = '','',''

    return year, month, time_of_day

def wrangleWeatherData(weather_df):
    #There are negative time values that must be removed.
    weather_df = weather_df[weather_df['currently_time'] > 0]

    #Convert time to format shared by crime data.
    weather_df['crime_time_format'] = weather_df['currently_time'].apply(convertTime)

    #Rename and drop columns.
    rename = dict(currently_apparentTemperature = 'apparent_temp',
    currently_cloudCover = 'cloud_cover',
    currently_dewPoint = 'dew_point',
    currently_humidity = 'humidity',
    currently_icon = 'icon',
    currently_precipIntensity = 'percip_intensity',
    currently_precipProbability = 'percip_probability',
    currently_precipType = 'percip_type',
    currently_pressure = 'pressure',
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
    weather_df.drop(labels=['index','code','summary','icon','error','percip_type'], axis='columns', inplace=True)

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
    census_df.rename(columns=dict(Year = 'census_year'), inplace=True)

    #Create an index to merge with crime data.
    census_df['index'] = census_df['Tract'] + census_df['BlockGroup'] + census_df['census_year']
    census_df_nodup = census_df.drop_duplicates(subset='index')

    return census_df_nodup

def wrangleCrimeData(crime_df):
    crime_df[['year','month','tod']] = crime_df.apply(assign_tod, axis=1, result_type='expand')

    return crime_df

def aggregateCrimeWeather(crime_weather_df,crime_type=None):
    agg_vars = ['offensegroup', 'apparent_temp', 'cloud_cover', 'dew_point', 'humidity',
                'percip_intensity', 'percip_probability', 'pressure', 'temperature', 'uv_index',
                'visibility', 'wind_bearing', 'wind_gust', 'wind_speed']

    agg_dict = dict()

    for var in agg_vars:
        if var == 'offensegroup':
            agg_dict[var] = 'size'
        else:
            agg_dict[var] = 'mean'

    if crime_type == None:
        crime_weather_agg = crime_weather_df.groupby(by=['BLOCK_GROUP','year','month','tod'], as_index=False).agg(agg_dict)
    else:
        crime_weather_agg = crime_weather_df.loc[crime_weather_df['offensegroup'] == crime_type] \
        .groupby(by=['BLOCK_GROUP','year','month','tod'], as_index=False).agg(agg_dict)

    crime_weather_agg['index'] = crime_weather_agg['BLOCK_GROUP'] + crime_weather_agg['year'].astype('str')
    crime_weather_agg['index'] = crime_weather_agg['index'].str.replace(" ","")

    return crime_weather_agg

def createDB(data):
    #Open connection and create cursor.
    conn = sqlite3.connect('../data/crime_census_weather_test.db')
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

    crime_weather_mr = crime_df_wr.merge(weather_df_wr,
                                         how='left',
                                         left_on=['LATITUDE','LONGITUDE','START_DATE'],
                                         right_on=['weather_latitude','weather_longitude','crime_time_format'])

    crime_weather_agg = aggregateCrimeWeather(crime_weather_mr, crime_type='violent')

    crime_weather_census = crime_weather_agg.merge(census_df_wr, how='left', on='index')

    createDB(crime_weather_census)

if __name__ == '__main__':
    main()
