"""
This program ingests WMATA bus stop locations and rail entrances.
"""
import json
import pandas as pd
import requests
import sqlite3

"""
Make an API call for a particular time/date/location of crime, return as DataFrame.
"""
def wmataAPICall(method):
    if method.upper() == 'BUS':
        url = 'https://api.wmata.com/Bus.svc/json/jStops'
        col = 'Stops'
    elif method.upper() == 'RAIL':
        url = 'https://api.wmata.com/Rail.svc/json/jStationEntrances'
        col = 'Entrances'
    else:
        print("Method not recognized.")

    api_key = '1aea694be6f947a2bae87dc8a70ae68f'
    headers = {'api_key': api_key}

    try:
        response = requests.get(url, headers=headers)
    except:
        print("Could not make request.")

    try:
        response_json = response.json()
    except:
        response_json = {}

    wmata_data_df = pd.io.json.json_normalize(response_json[col])

    return wmata_data_df

"""
Write to Database Table
"""
def writeDatabaseFile(dataframe, method):
    #Connect to DB table.
    conn = sqlite3.connect('../data/wmata-data/{}_data.db'.format(method))
    c = conn.cursor()

    #Drop table if exists.
    c.execute("drop table if exists {}_data".format(method))

    #Create Tables
    if method == 'bus':
        c.execute('''create table bus_data
        (Latitude int,
        Longitude int,
        Stop_Name varchar(225),
        Routes_Available varchar(225),
        Stop_ID)''')
    elif method == 'rail':
        c.execute('''create table rail_data
        (Description varchar(225),
        ID,
        Latitude int,
        Longitude int,
        Station_Entrance varchar(225),
        StationCode1 varchar(225),
        StationCode2 varchar(225))''')

    #Write to Tables
    if method == 'bus':
        for index, row in dataframe.iterrows():
            data = (row[0], row[1], str(row[2]), ', '.join(row[3]), row[4])
            c.execute("insert into bus_data values (?,?,?,?,?)", data)
    elif method == 'rail':
        for index, row in dataframe.iterrows():
            c.execute("insert into rail_data values (?,?,?,?,?,?,?)", tuple(row))

    #Commit and close connection.
    conn.commit()
    conn.close()

"""
The driver function.
"""
def main(method):
    wmata_data = wmataAPICall(method=method)
    writeDatabaseFile(dataframe=wmata_data, method=method)

if __name__ == "__main__":
    main('rail')
    main('bus')
