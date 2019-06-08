################################################################################
## Imports
################################################################################

import pandas as pd
pd.set_option('display.max_columns', 100)


################################################################################
## Constants
################################################################################

crimeData = pd.read_csv('dc-crime-data.csv')
censusData = pd.read_csv('FinalBlockGroupData.csv')


################################################################################
## Functions
################################################################################

def cleanCensusData():

    censusData[(censusData['TotalPop'] < 0)] = censusData['TotalPop'].mean()
    censusData[(censusData['TPopMargin'] < 0)] = censusData['TPopMargin'].mean()
    censusData[(censusData['UnWgtSampleCtPop'] < 0)] = censusData['UnWgtSampleCtPop'].mean()
    censusData[(censusData['PerCapitaIncome'] < 0)] = censusData['PerCapitaIncome'].mean()
    censusData[(censusData['PerCapIncMargin'] < 0)] = censusData['PerCapIncMargin'].mean()
    censusData[(censusData['MedianHouseholdInc'] < 0)] = censusData['MedianHouseholdInc'].mean()
    censusData[(censusData['MedHouseholdIncMargin'] < 0)] = censusData['MedHouseholdIncMargin'].mean()
    censusData[(censusData['MedianAge'] < 0)] = censusData['MedianAge'].mean()
    censusData[(censusData['MedianAgeMargin'] < 0)] = censusData['MedianAgeMargin'].mean()
    censusData[(censusData['HousingUnits'] < 0)] = censusData['HousingUnits'].mean()
    censusData[(censusData['HousingUnitsMargin'] < 0)] = censusData['HousingUnitsMargin'].mean()
    censusData[(censusData['UnweightedSampleHousingUnits'] < 0)] = censusData['UnweightedSampleHousingUnits'].mean()


    censusData.fillna(censusData.mean())
    del censusData['Unnamed: 0']
    censusData['BlockGroup'] = censusData['BlockGroup'] = censusData['BlockGroup'].astype(str).replace(']]', '', regex=True)

    censusData['Tract'] = censusData['Tract'].astype(str).replace('\.0', '', regex=True)
    censusData['Tract'] = censusData['Tract'].apply(lambda x: x.zfill(6))
    censusData['BlockGroup'] = censusData['BlockGroup'].astype(str).replace('\.0', '', regex=True)
    censusData['Year'] = censusData['Year'].astype(str).replace('\.0', '', regex=True)
    crimeData['YEAR'] = crimeData['YEAR'].astype(str)
    crimeData['BLOCK_GROUP'] = crimeData['BLOCK_GROUP'].astype(str)

def indexThenMerge():

    censusData['index'] = censusData['Tract'] + " " + censusData['BlockGroup'] + " " + censusData['Year']
    crimeData['index'] = crimeData['BLOCK_GROUP'] + " " + crimeData['YEAR']

    df_merge = crimeData.merge(censusData.drop_duplicates(subset=['index']), how='left', on='index', indicator=True)

def main():
    cleanCensusData()
    indexThenMerge()

################################################################################
## Execution
################################################################################

if __name__ == '__main__':
    main()
