################################################################################
## Imports
################################################################################
import datetime
import numpy as np
import pandas as pd
import sqlite3
from sklearn import preprocessing
import zipfile

################################################################################
## Import Data
################################################################################
def importCensusData():
    '''
    Import from DB table and return a DataFrame.
    '''
    conn = sqlite3.connect('../data/census-data/census_bg.db')
    census_df = pd.read_sql('''select * from census_blockgroup''', conn)
    conn.close()

    return census_df

def importWeatherData():
    '''
    Import the weather data by accessing DB table.
    '''
    conn = sqlite3.connect('../data/weather-data/weather_data.db')
    weather_df = pd.read_sql('''select * from weather_data''', conn)
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
    '''
    Convert time elapsed from 1/1/1970 to YYYY-MM-DTHH:MM:SS.MMM format.
    '''
    time = datetime.datetime.fromtimestamp(row).strftime('%Y-%m-%dT%H:%M:%S.000')

    return time

def assignTod(row):
    '''
    Assign time of day (TOD) based on the hour.
    '''
    try:
        timestamp = pd.Timestamp(row['START_DATE'])

        year = timestamp.year
        month = timestamp.month
        day = timestamp.day
        start_hour = timestamp.time().hour

        if 23 <= start_hour:
            tod_cat, tod_num = 'Midnight', 8
        if 0 <= start_hour < 2:
            tod_cat, tod_num = 'Midnight', 8
        elif 2 <= start_hour < 5:
            tod_cat, tod_num = 'Late Night', 1
        elif 5 <= start_hour < 8:
            tod_cat, tod_num = 'Early Morning', 2
        elif 8 <= start_hour < 11:
            tod_cat, tod_num = 'Morning', 3
        elif 11 <= start_hour < 14:
            tod_cat, tod_num = 'Afternoon', 4
        elif 14 <= start_hour < 17:
            tod_cat, tod_num = 'Mid Afternoon', 5
        elif 17 <= start_hour < 20:
            tod_cat, tod_num = 'Evening', 6
        elif 20 <= start_hour < 23:
            tod_cat, tod_num = 'Night', 7
    except:
        year, month, day, tod_cat, tod_num = '','','','',np.nan

    return year, month, day, tod_cat, tod_num

def wrangleWeatherData(weather_df):
    '''
    Wrangle weather data to prepare to merge with crime and census data.
    '''
    #There are negative time values that must be removed.
    weather_df = weather_df[weather_df['currently_time'] > 0]

    #Convert time to format shared by crime data.
    weather_df['crime_time_format'] = weather_df['currently_time'].apply(convertTime)

    #Rename and drop columns.
    rename_cols = dict(currently_apparentTemperature = 'apparent_temp',
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
    drop_cols = ['index','code','summary','icon','error','percip_type']

    weather_df.rename(columns=rename_cols, inplace=True)
    weather_df.drop(labels=drop_cols, axis='columns', inplace=True)

    return weather_df

def wrangleCensusData(census_df):
    '''
    Wrangle US Census data.
    '''
    #Fill NA values and values less than 0 with the mean of values greater than zero.
    columns = ['TotalPop','TPopMargin','UnWgtSampleCtPop','PerCapitaIncome','MedianHouseholdInc',
               'MedianAge','HousingUnits','UnweightedSampleHousingUnits']

    for col in columns:
        #Edit string (object) columns to numeric (float).
        if census_df[col].dtypes == 'object':
            numeric_column = pd.to_numeric(census_df[col], errors = 'coerce')
            census_df[col] =  numeric_column

        #Calculate Mean.
        mean = census_df[census_df[col] > 0][col].mean()

        #Fill NA with a dictionary of column name and the mean value.
        census_df.fillna(value={col: mean}, inplace=True)

        #Replace values less than zero with the mean.
        census_df[census_df[col] < 0] = mean

    #Keep population values greater than zero.
    census_df = census_df.loc[census_df['TotalPop'] > 0]

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
    '''
    Assign time-of-day categories to the reported start time.
    '''
    crime_df[['year','month','day','tod_cat', 'tod_num']] = crime_df.apply(assignTod, axis=1, result_type='expand')

    return crime_df

def setWeekday(row):
    '''
    Set Weekday value (0-6).
    '''
    try:
        date = '{}-{}-{}'.format(str(row['year']), str(row['month']), str(row['day']))
        weekday = pd.Timestamp(date).weekday()
    except:
        weekday = np.nan

    return weekday

def aggregateCrimeWeather(crime_weather_df,by_crime_type=False):
    '''
    Aggregate crime incidents and weather variables to block group area level.
    '''
    #Identify variables for aggregation. Count instances of REPORT_DAT and average weather variables.
    agg_vars = ['REPORT_DAT', 'apparent_temp', 'cloud_cover', 'dew_point', 'humidity',
                'percip_intensity', 'percip_probability', 'pressure', 'temperature', 'uv_index',
                'visibility', 'wind_bearing', 'wind_gust', 'wind_speed']

    agg_dict = dict()

    #Set aggregation method for each variable.
    for var in agg_vars:
        if var == 'REPORT_DAT':
            agg_dict[var] = 'size'
        else:
            agg_dict[var] = 'mean'

    #Based on user choice, aggregate by offensegroup or by all offenses.
    if by_crime_type == False:
        crime_weather_agg = crime_weather_df.groupby(by=['BLOCK_GROUP','year','month','day','tod_cat','tod_num'], as_index=False).agg(agg_dict)
    else:
        crime_weather_agg = crime_weather_df.groupby(by=['BLOCK_GROUP','year','month','day','tod_cat','tod_num','offensegroup'], as_index=False).agg(agg_dict)

    #Rename REPORT_DAT to crime_counts.
    rename_cols = dict(REPORT_DAT = 'crime_counts')
    crime_weather_agg.rename(columns=rename_cols, inplace=True)

    #Set week day.
    crime_weather_agg['weekday'] = crime_weather_agg[['year', 'month', 'day']].apply(setWeekday, axis=1)

    #Create Index variable to merge with US Census data.
    crime_weather_agg['index'] = crime_weather_agg['BLOCK_GROUP'] + crime_weather_agg['year'].astype('str')
    crime_weather_agg['index'] = crime_weather_agg['index'].str.replace(" ","")

    return crime_weather_agg

def calculateCrimeRates(crime_weather_agg):
    '''
    Calculate crime rates per 100,000 people.
    '''
    #Remove blank Census data.
    crime_weather_agg_na = crime_weather_agg.dropna(axis='index', how='any', subset=['TotalPop']).reset_index(drop=True)

    #Calculate crime rates per 100,000 people.
    crime_weather_agg_na['crime_rate'] = (crime_weather_agg_na['crime_counts'] / crime_weather_agg_na['TotalPop'])*100000

    #Standardize crime rates.
    standardize = crime_weather_agg_na[['crime_rate']]
    power = preprocessing.PowerTransformer(method='box-cox', standardize=False)
    crime_weather_agg_na['crs'] = power.fit_transform(standardize)

    return crime_weather_agg_na

def classifyCrimeRates(crime_rates, stats):
    range_high = float(stats['mean'] + 2*stats['std'])
    range_low = float(stats['mean'] - 2*stats['std'])
    range_mid_high = float(stats['mean'] + stats['std'])
    range_mid_low = float(stats['mean'] - stats['std'])
    cr = float(crime_rates['crs'])

    if cr >= range_high:
        crime_rate_cat = 'High'
    if cr >= range_mid_high and cr < range_high:
        crime_rate_cat = 'Med-High'
    if cr >= range_mid_low and cr < range_mid_high:
        crime_rate_cat = 'Med'
    if cr >= range_low and cr < range_mid_low:
        crime_rate_cat = 'Low-Med'
    if cr < range_low:
        crime_rate_cat = 'Low'

    return crime_rate_cat
################################################################################
## EXPORT
################################################################################
def createDB(data, table_name):
    '''
    Write dataframes to database tables.
    '''
    #Open connection and create cursor.
    conn = sqlite3.connect('../data/crime_census_weather_tod.db')
    c = conn.cursor()

    #If table already exists, drop it before writing to it.
    c.execute("drop table if exists {}".format(table_name))

    #Write to the table.
    data.to_sql(table_name, conn)

    #Commit and close connection.
    conn.commit()
    conn.close()

################################################################################
## Driver
################################################################################
def main():
    #Import each input dataset.
    crime_df = importCrimeData()
    census_df = importCensusData()
    weather_df = importWeatherData()

    #Wrangle each input dataset.
    weather_df_wr = wrangleWeatherData(weather_df)
    census_df_wr = wrangleCensusData(census_df)
    crime_df_wr = wrangleCrimeData(crime_df)

    #Merge weather input and crime input.
    crime_weather_mr = crime_df_wr.merge(weather_df_wr,
                                         how='left',
                                         left_on=['LATITUDE','LONGITUDE','START_DATE'],
                                         right_on=['weather_latitude','weather_longitude','crime_time_format'])

    #Aggregate weather input and crime input to Census Block Group level.
    crime_weather_agg_ac = aggregateCrimeWeather(crime_weather_mr)
    crime_weather_agg_ct = aggregateCrimeWeather(crime_weather_mr, by_crime_type=True)

    #Merge weather-crime aggregated dataset with US Census input dataset.
    crime_weather_census_ac = crime_weather_agg_ac.merge(census_df_wr, how='left', on='index')
    crime_weather_census_ct = crime_weather_agg_ct.merge(census_df_wr, how='left', on='index')

    #Calculate and standardize crime rates per 100,000 people.
    crime_rate_ac = calculateCrimeRates(crime_weather_census_ac)
    crime_rate_ct = calculateCrimeRates(crime_weather_census_ct)

    #Classify Crime Rates (Classification Target)
    stats_ac = crime_rate_ac[['crs']].describe().transpose()
    crime_rate_ac['crime_rate_cat'] = crime_rate_ac[['crs']].apply(classifyCrimeRates, args=(stats_ac[['mean','std']],), axis=1)

    #Export to DB Table.
    createDB(data=crime_rate_ac, table_name='all_crimes')
    createDB(data=crime_rate_ct, table_name='by_crime_type')

if __name__ == '__main__':
    main()
