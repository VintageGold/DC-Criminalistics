"""
Title: Census API Script
About: Pulls data from the Census API
Use the def main to list variables and desired date range
"""

################################################################################
## Imports
################################################################################

import requests
import json
import csv

################################################################################
## Variables/Constatns
################################################################################

myKey = 'INSERT VALID KEY HERE'
byBlockGroup = '&for=block%20group:*&in=state:11&in=county:001&in=tract:*'
byTract = '&for=tract:*&in=state:11&in=county:001'
blockGroupList = []
tractList = []

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
                filename = "tmp/BlockGroup/" + url[x][28:32] + ".txt"
                myFile = open(filename, "w")
                myFile.write(r.text)
                myFile.close()
            elif type == "Tract":
                filename = "tmp/Tract/" + url[x][28:32] + ".txt"
                myFile = open(filename, "w")
                myFile.write(r.text)
                myFile.close()

def main():
    """
    Main execution function to perform required actions
    Enter variables, and date range
    """

    #Construct URL for block group:
    blockGroupURLs = prepBlockGroupURL(('B01003_001E',
                                        'B00001_001E',
                                        'B19301_001E',
                                        'B19013_001E',
                                        'B01002_001E',
                                        'B25001_001E',
                                        'B00002_001E'), (2009, 2017))

    tractURLs = prepTractURL(('B01003_001E',
                              'B00001_001E',
                              'B19301_001E',
                              'B19013_001E',
                              'B01002_001E',
                              'B25001_001E',
                              'B00002_001E'), (2009, 2017))


    requestData(blockGroupURLs, "BlockGroup")
    requestData(tractURLs, "Tract")



################################################################################
## Execution
################################################################################

if __name__ == '__main__':
    main()
