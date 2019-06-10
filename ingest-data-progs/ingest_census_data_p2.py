################################################################################
## Import Libraries
################################################################################
import os
import pandas as pd
import sqlite3

################################################################################
## Functions
################################################################################
def checkDirectory(directory):
    """
    Check if directory exists, otherwise create it.
    """
    if not os.path.exists(directory):
        os.makedirs(directory)
    else:
        print('Directory already exists, exiting.')

def importCensusData(directory, headers=None):
    """
    Import Census data and return a dataframe.
    """
    data = []

    with open(directory, 'r') as infile:
        lines = infile.readlines()

        for index, line in enumerate(lines):
            if index == 0:
                columns = line.replace('],', '').replace('[', '').replace('\"', '').replace(']]', '').replace('\n','').split(',')
            else:
                row = line.replace('],', '').replace('[', '').replace('\"', '').replace(']]', '').replace('\n','').split(',')
                data.append(row)

    if headers==None:
        column_names = column
    else:
        column_names = headers

    census_df = pd.DataFrame(data=data, columns=column_names)

    return census_df

def compileCensusData(directory, years, headers):
    """
    Compile all years of Census data, return dataframe.
    """
    census_df_list = []

    for index, year in enumerate(years):
        file_path = os.path.join(directory, years[int(index)] + ".txt")
        census_df = importCensusData(directory=file_path, headers=headers)
        census_df['Year_census'] = int(year)
        census_df_list.append(census_df)

    census_bg = pd.concat(census_df_list).reset_index(drop=True)

    return census_bg

def outputData(data):
    """
    Output Census data to SQL database table.
    """
    conn = sqlite3.connect('../data/census-data/census_bg.db')
    c = conn.cursor()

    c.execute("drop table if exists census_blockgroup")
    df = data.rename(index=str)
    df.to_sql('census_blockgroup', conn)

    conn.commit()
    conn.close()

def main():
    """
    Main execution function to perform required actions.
    """
    #Set Years
    bg_years = ['2009', '2013', '2014', '2015', '2016', '2017']

    #Set header names for Block Group database table.
    column_names = ['TotalPop','TotalPopMargin','UnWgtSampleCtPop','PerCapitaIncome',
                    'PerCapitalIncomeMargin','MedianHouseholdInc','MedianHouseholdIncMargin','MedianAge',
                    'MedianAgeMargin','HousingUnits','HousingUnitsMargin','UnweightedSampleHousingUnits',
                    'State','County','Tract','BlockGroup']

    #Check and Create Directories, necessary.
    folder_path = os.path.dirname('../data/census-data/BlockGroup/')
    checkDirectory(folder_path)

    #Compile all years of raw Census data into one Pandas dataframe.
    census_df = compileCensusData(directory=folder_path, years=bg_years, headers=column_names)

    #Output data to database table.
    outputData(census_df)

################################################################################
## Execution
################################################################################
if __name__ == '__main__':
    main()
