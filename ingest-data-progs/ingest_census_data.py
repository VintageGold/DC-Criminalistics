"""
Title: Census API Script
About: Pulls data from the Census API
Use the def main to list variables and desired date range
"""

################################################################################
## Imports
################################################################################

import requests
import csv
import os
import shutil
import sys
import time

################################################################################
## Variables/Constatns
################################################################################

#Census API Key:
myKey = 'INSERT KEY HERE'

#String required in API request
byBlockGroup = '&for=block%20group:*&in=state:11&in=county:001&in=tract:*'
byTract = '&for=tract:*&in=state:11&in=county:001'

#Used in definitions below for creating list for tract and block group
blockGroupList = []
tractList = []

#directories to be created where API pulls will go
file_path_bg = '../data/census-data/BlockGroup'
directory_bg = os.path.dirname(file_path_bg)

file_path_tr = '../data/census-data/Tract/'
directory_tr = os.path.dirname(file_path_tr)

################################################################################
## Functions
################################################################################

def prepBlockGroupURL(censusVars, years):
    """
    Constructs the URLs necessary for data by block group
    Returns a list of URLs specified by variable and years in the main def
    """
    blockGroupURL = ''
    for x in range(len(censusVars)):
        if x < len(censusVars) - 1:
            blockGroupURL = blockGroupURL + censusVars[x] + ","
        else:
            blockGroupURL = blockGroupURL + censusVars[x]
    for x in range(years[0], years[1]+1):
        if x == 2009:
            call = "https://api.census.gov/data/" + str(x) + "/acs5?get=" + blockGroupURL + byBlockGroup + "&key=" + myKey
        else:
            call = "https://api.census.gov/data/" + str(x) + "/acs/acs5?get=" + blockGroupURL + byBlockGroup + "&key=" + myKey
        blockGroupList.append(call)
    return blockGroupList

def prepTractURL(censusVars, years):
    """
    Constructs the URLs necessary for data by tract
    Returns a list of URLs specified by variable and years in the main def
    """
    tractURL = ''
    for x in range(len(censusVars)):
        if x < len(censusVars) - 1:
            tractURL = tractURL + censusVars[x] + ","
        else:
            tractURL = tractURL + censusVars[x]
    for x in range(years[0], years[1]+1):
        if x == 2009:
            call = "https://api.census.gov/data/" + str(x) + "/acs5?get=" + tractURL + byTract + "&key=" + myKey
        else:
            call = "https://api.census.gov/data/" + str(x) + "/acs/acs5?get=" + tractURL + byTract + "&key=" + myKey
        tractList.append(call)
    return tractList

def requestData(url, type):
    """
    Uses requests library to call the Census API based on the passed list of tractURLs
    Results are written to file as .txt files
    Note, code expects /tmp/BlockGroup/ and /tmp/Tract to be valid paths
    """
    for x in range(len(url)):
        r = requests.get(url[x])

        if r.status_code != 200:
            print("\nSorry, something went wrong with this call:\n" + r.url)
            print("Status Code:" + str(r.status_code))
            print(r.text)

        elif r.status_code == 200:
            if type == "BlockGroup":
                filename = file_path_bg + url[x][28:32] + ".txt"
                myFile = open(filename, "w")
                myFile.write(r.text)
                myFile.close()
            elif type == "Tract":
                filename = file_path_tr + url[x][28:32] + ".txt"
                myFile = open(filename, "w")
                myFile.write(r.text)
                myFile.close()

def main():
    """
    Main execution function to perform required actions
    Enter variables, and date range
    """
    print("********************************************************************************")
    print("Note, Census ACS 5yr block group data is not available for 2010, 2011, and 2012.")
    print("So, if you're trying to pull those years, you're about to get some errors.")
    print("That's ok though, the other years should be just fine")
    print("********************************************************************************")
    time.sleep(7)

    #Creating directory path for raw block group data
    if not os.path.exists(directory_bg):
        os.makedirs(directory_bg)
    else:
        print("Block group directory already exists, exiting.")
        sys.exit()

    #Creating directory path for raw tract data
    if not os.path.exists(directory_tr):
        os.makedirs(directory_tr)
    else:
        print("Tract directory already exists, exiting.")
        sys.exit()


    #Construct URL for block group:
    blockGroupURLs = prepBlockGroupURL(('B01003_001E',
                                        'B01003_001M',
                                        'B00001_001E',
                                        'B19301_001E',
                                        'B19301_001M',
                                        'B19013_001E',
                                        'B19013_001M',
                                        'B01002_001E',
                                        'B01002_001M',
                                        'B25001_001E',
                                        'B25001_001M',
                                        'B00002_001E'), (2009, 2017))

    tractURLs = prepTractURL(('B01003_001E',
                              'B01003_001M',
                              'B00001_001E',
                              'B19301_001E',
                              'B19301_001M',
                              'B19013_001E',
                              'B19013_001M',
                              'B01002_001E',
                              'B01002_001M',
                              'B25001_001E',
                              'B25001_001M',
                              'B00002_001E'), (2009, 2017))


    requestData(blockGroupURLs, "BlockGroup")
    requestData(tractURLs, "Tract")

################################################################################
## Execution
################################################################################

if __name__ == '__main__':
    main()
