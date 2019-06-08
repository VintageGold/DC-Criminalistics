################################################################################
## Imports
################################################################################
import datetime
import pandas as pd
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

def assign_tod(row):
    '''
    Assign time of day (TOD) based on the hour.
    '''
    try:
        timestamp = pd.Timestamp(row['START_DATE'])

        year = timestamp.year
        month = timestamp.month
        start_hour = timestamp.time().hour

        if 23 <= start_hour:
            time_of_day = 'Midnight'
        if 0 <= start_hour < 2:
            time_of_day = 'Midnight'
        elif 2 <= start_hour < 5:
            time_of_day = 'Late Night'
        elif 5 <= start_hour < 8:
            time_of_day = 'Early Morning'
        elif 8 <= start_hour < 11:
            time_of_day = 'Morning'
        elif 11 <= start_hour < 14:
            time_of_day = 'Afternoon'
        elif 14 <= start_hour < 17:
            time_of_day = 'Mid Afternoon'
        elif 17 <= start_hour < 20:
            time_of_day = 'Evening'
        elif 20 <= start_hour < 23:
            time_of_day = 'Night'
    except:
        year, month, time_of_day = '','',''

    return year, month, time_of_day

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
        crime_weather_agg = crime_weather_df.groupby(by=['BLOCK_GROUP','year','month','tod'], as_index=False).agg(agg_dict)
    else:
        crime_weather_agg = crime_weather_df.groupby(by=['BLOCK_GROUP','year','month','tod','offensegroup'], as_index=False).agg(agg_dict)

    #Rename REPORT_DAT to crime_counts.
    rename_cols = dict(REPORT_DAT = 'crime_counts')

    crime_weather_agg.rename(columns=rename_cols, inplace=True)

    #Create Index variable to merge with US Census data.
    crime_weather_agg['index'] = crime_weather_agg['BLOCK_GROUP'] + crime_weather_agg['year'].astype('str')
    crime_weather_agg['index'] = crime_weather_agg['index'].str.replace(" ","")

    return crime_weather_agg

###############################################################################
## EXPORT
###############################################################################
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

#############################################################################
## Driver
#############################################################################
def main():
    #Import each dataset.
    crime_df = importCrimeData()
    census_df = importCensusData()
    weather_df = importWeatherData()

    #Wrangle each dataset.
    weather_df_wr = wrangleWeatherData(weather_df)
    census_df_wr = wrangleCensusData(census_df)
    crime_df_wr = wrangleCrimeData(crime_df)

    #Merge Weather and Crime
    crime_weather_mr = crime_df_wr.merge(weather_df_wr,
                                         how='left',
                                         left_on=['LATITUDE','LONGITUDE','START_DATE'],
                                         right_on=['weather_latitude','weather_longitude','crime_time_format'])

    #Aggregate Weather and Crime to Blockgroup Level
    crime_weather_agg = aggregateCrimeWeather(crime_weather_mr)
    crime_weather_agg_ct = aggregateCrimeWeather(crime_weather_mr, by_crime_type=True)

    #Merge Weather-Crime Dataset with US Census Dataset
    crime_weather_census = crime_weather_agg.merge(census_df_wr, how='left', on='index')
    crime_weather_census_ct = crime_weather_agg_ct.merge(census_df_wr, how='left', on='index')

    #Calculate Crime Rates
    crime_weather_census['crime_rate'] = crime_weather_census['crime_counts'] / crime_weather_census['TotalPop']
    crime_weather_census_ct['crime_rate'] = crime_weather_census_ct['crime_counts'] / crime_weather_census_ct['TotalPop']

    #Export to DB Table
    createDB(data=crime_weather_census, table_name='all_crimes')
    createDB(data=crime_weather_census_ct, table_name='by_crime_type')

if __name__ == '__main__':
    main()
