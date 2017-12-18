# -*- coding: utf-8 -*-

from pymongo import MongoClient
import pandas as pd
import requests
import json
import twd97

# set mongodb ip and port
client = MongoClient('140.112.41.157',27018)

# set mongodb database and collection
db = client['cloud-final']
collection = db['parkingInfo']

# get data from open source
link = "http://data.ntpc.gov.tw/api/v1/rest/datastore/382000000A-000225-002"
req = requests.get(link)
data = json.loads(req.text)

# save data in dataframe
df_park = pd.DataFrame.from_records(data['result']['records'])

# transfrom TWD97 to normal latitude and longitude
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

# insert data to mongodb
for index,row in df_park.iterrows():   
    data = {"park_id" : row['ID'] ,
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