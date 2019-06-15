import sqlite3
import pandas as pd
import censusgeocode as cg

conn = sqlite3.connect("DC-Criminalistics/data/cabi-station-data/cabi_station_data.db")
cur = conn.cursor()
df = pd.read_sql_query("select * from cabi_station_data limit 10", conn)

census_block = []
census_blockgroup = []
census_tract = []

for row in df.itertuples(index=True, name='Pandas'):
	try:
		info = cg.coordinates(x=getattr(row, "lon"), y=getattr(row, "lat"))
		census_block.append(info['2010 Census Blocks'][0]['BLOCK'])
		census_blockgroup.append(info['2010 Census Blocks'][0]['BLKGRP'])
		census_tract.append(info['2010 Census Blocks'][0]['TRACT'])
	except ValueError:
		census_block.append(None)
		census_blockgroup.append(None)
		census_tract.append(None)


df['census_block'] = census_block
df['census_blockgroup'] = census_blockgroup
df['census_tract'] = census_tract

df.to_csv('cabi_reverse_geocode_out.csv')
