import pandas as pd 
import zipfile
import datetime

"""
This program writes DC Crime Data CSV to a dataframe, and adds date and time fields.
"""
def DCCrimeData(zippedcsv, csv):

    #Zipped CSV to dataframe
    zf = zipfile.ZipFile(zippedcsv)
    df = pd.read_csv(zf.open(csv))

    #Covert START_DATE to datetime
    df['START_DATE'] = pd.to_datetime(df['START_DATE'])

    #Add date and time fields
    df['START_YEAR'] = df['START_DATE'].dt.year
    df['START_MONTH'] = df['START_DATE'].dt.month
    df['START_DAY'] = df['START_DATE'].dt.day
    df['START_DAY_OF_WEEK'] = df['START_DATE'].dt.weekday_name
    df['START_DAY_WEEK_OF_YEAR'] = df['START_DATE'].dt.week
    df['START_TIME'] = df['START_DATE'].dt.time
    df['START_DAY_HOUR'] = df['START_DATE'].dt.hour
    df['START_DAY_MINUTE'] = df['START_DATE'].dt.minute

    #Need to update times of day categories to something that makes more sense
    df['START_TIME_CATEGORY'] = pd.cut(df['START_DATE'].dt.hour,[0,6,12,18,24],labels=['Night','Morning','Afternoon','Evening'],include_lowest=True)

if __name__ == "__main__":
    DCCrimeData('dc-crime-data.csv.zip','dc-crime-data.csv')
