# -*- coding: utf-8 -*-

from pymongo import MongoClient
import pandas as pd
import requests
import json
import twd97

client = MongoClient('140.112.41.157',27018)
db = client['cloud-final']
collection = db['parkingInfo']

link = "http://data.ntpc.gov.tw/api/v1/rest/datastore/382000000A-000225-002"
req = requests.get(link)
data = json.loads(req.text)

df_park = pd.read_csv("park_data.csv",encoding='utf-8')
col_name = df_park.columns

latitude = []
longitude = []
TWD97Y = df_park["TW97Y"].values
TWD97X = df_park["TW97X"].values
for i in range(len(TWD97X)):
    lat, long = twd97.towgs84(TWD97X[i],TWD97Y[i])
    latitude.append(lat)
    longitude.append(long)
df_park["latitude"] = latitude
df_park["longitude"] = longitude

for index,row in df_park.iterrows():
    
    data = {"park_id" : row[col_name[0]] ,
            "park_area" : row['AREA'] ,
            "park_name" : row['NAME'] ,
            "park_summary" : row['SUMMARY'] ,
            "park_address" : row['ADDRESS'] ,
            "park_payex" : row['PAYEX'] ,
            "park_servicetime" : row['SERVICETIME'] ,
            "park_latitude" : row['latitude'] ,
            "park_longitude" : row['longitude'] ,
            "park_totalcar" : row['TOTALCAR'] }
    post_id = collection.insert_one(data).inserted_id
    print(post_id)
