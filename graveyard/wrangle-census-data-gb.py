################################################################################
## Import Libraries
################################################################################
import zipfile
import pandas as pd
import csv
import json
import datetime

################################################################################
## Import Data
################################################################################
def importCrimeData():
    #Import the crime data by unpacking the ZIP file, converting to CSV, then returning DataFrame.
    crime_zip = zipfile.ZipFile("../dc-crime-data/dc-crime-data.csv.zip", mode='r')
    crime_csv = crime_zip.open('dc-crime-data.csv')
    crime_df = pd.read_csv(crime_csv)

    return crime_df

def importCensusData():
    #Import directly from CSV and return a DataFrame.
    census_df = pd.read_csv('../census-data/Clean/BlockGroupClean/BlockGroupData.csv')

    return census_df

################################################################################
## Functions
################################################################################
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

################################################################################
## Driver
################################################################################
def main():
    crime_df = importCrimeData()
    census_df = importCensusData()
    census_df_wr = wrangleCensusData(census_df)
    crime_df_wr = wrangleCrimeData(crime_df)

    crime_census_mr = crime_df.merge(census_df.drop_duplicates(subset=['index']), how='left', on='index', indicator=True)

if __name__ == '__main__':
    main()
