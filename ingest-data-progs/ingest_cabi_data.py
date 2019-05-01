'''
Import Libraries
'''
import json
import pandas as pd
import requests
import sqlite3

'''
Pull CABI station locations from https://gbfs.capitalbikeshare.com/gbfs/en/station_information.json
'''
def cabiWebPull():
    url = 'https://gbfs.capitalbikeshare.com/gbfs/en/station_information.json'

    response = requests.get(url)
    data = response.json()

    cabi_data_df = pd.io.json.json_normalize(data['data']['stations'])

    return cabi_data_df

"""
Write to Database Table
"""
def writeDatabaseFile(dataframe):
    #Connect to DB table.
    conn = sqlite3.connect('../data/cabi-station-data/cabi_station_data.db')
    c = conn.cursor()

    #Drop table if exists.
    c.execute("drop table if exists cabi_station_data")

    dataframe = dataframe.drop(columns=['eightd_station_services','eightd_has_key_dispenser', 'rental_methods'])
    dataframe.to_sql('cabi_station_data',conn)

    #Commit and close connection.
    conn.commit()
    conn.close()

"""
The driver function.
"""
def main():
    cabi_locations = cabiWebPull()
    writeDatabaseFile(cabi_locations)

if __name__ == "__main__":
    main()
