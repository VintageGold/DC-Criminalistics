#Removes special characters from the Census data

################################################################################
## Import Libraries
################################################################################
import pandas as pd
import os
import shutil
import sys
import sqlite3

################################################################################
## Variables/Constatns
################################################################################

bgFiles = ('2009.txt', '2013.txt', '2014.txt', '2015.txt', '2016.txt', '2017.txt')
tractFiles = ('2009.txt','2010.txt','2011.txt','2012.txt', '2013.txt', '2014.txt', '2015.txt', '2016.txt', '2017.txt')

#directories to be created where cleaned data goes
file_path_bg = 'CleanedCensusData/BlockGroup/'
directory_bg = os.path.dirname(file_path_bg)

file_path_tr = 'CleanedCensusData/Tract/'
directory_tr = os.path.dirname(file_path_tr)

################################################################################
## Functions
################################################################################

def cleanFile(inputFileName, outputFileName):
    """
    Removes special characters from the datasets
    """

    with open(r'' + inputFileName, 'r') as infile, \
         open(r'' + outputFileName, 'w') as outfile:
         data = infile.read()
         data = data.replace('],', '')
         data = data.replace('[', '')
         data = data.replace('\"', '')
         data = data.replace(']]', '')
         outfile.write(data)

def createSingleBlockGroupFile():
    data09 = pd.read_csv(file_path_bg + '2009.txt')
    data13 = pd.read_csv(file_path_bg + '2013.txt')
    data14 = pd.read_csv(file_path_bg + '2014.txt')
    data15 = pd.read_csv(file_path_bg + '2015.txt')
    data16 = pd.read_csv(file_path_bg + '2016.txt')
    data17 = pd.read_csv(file_path_bg + '2017.txt')

    data09['Year_census'] = 2009
    data13['Year_census'] = 2013
    data14['Year_census'] = 2014
    data15['Year_census'] = 2015
    data16['Year_census'] = 2016
    data17['Year_census'] = 2017

    frames = [data09, data13, data14, data15, data16, data17]

    df_bg = pd.concat(frames)

    df_bg.columns = ['TotalPop',
                     'TPopMargin',
                     'UnWgtSampleCtPop',
                     'PerCapitaIncome',
                     'PerCapIncMargin',
                     'MedianHouseholdInc',
                     'MedHouseholdIncMargin',
                     'MedianAge',
                     'MedianAgeMargin',
                     'HousingUnits',
                     'HousingUnitsMargin',
                     'UnweightedSampleHousingUnits',
                     'State',
                     'County',
                     'Tract',
                     'BlockGroup',
                     'Year']

    return df_bg

def createdb(data):
    df = data.rename(index=str)
    conn = sqlite3.connect('CleanedCensusData/census_bg.db')
    df.to_sql('census_blockgroup', conn)

def main():
    """
    Main execution function to perform required actions
    """

    #Create directory for cleaned block group data
    if not os.path.exists(directory_bg):
        os.makedirs(directory_bg)
    else:
        print('Block group directory already exists, exiting.')
        sys.exit()

    #Create directory for cleaned tract data
    if not os.path.exists(directory_tr):
        os.makedirs(directory_tr)
    else:
        print('Tract directory already exists, exiting.')
        sys.exit()

    #Reading the raw data, and creating seperate cleaned files
    for x in range(len(bgFiles)):
        inputFile = 'RawCensusData/BlockGroup/' + bgFiles[x]
        outputFile = file_path_bg + bgFiles[x]
        cleanFile(inputFile, outputFile)

    for x in range(len(tractFiles)):
        inputFile = 'RawCensusData/Tract/' + tractFiles[x]
        outputFile = file_path_tr + tractFiles[x]
        cleanFile(inputFile, outputFile)

    #Takes cleaned files and creates one single file
    #Script only handles block group data at this point
    df_bg_clean = createSingleBlockGroupFile()

    createdb(df_bg_clean)

################################################################################
## Execution
################################################################################

if __name__ == '__main__':
    main()
