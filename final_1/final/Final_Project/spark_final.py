# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import requests
import urllib
import json

if __name__ == "__main__":

	spark = SparkSession.builder.appName("FinalProject").getOrCreate()

    link = "http://data.ntpc.gov.tw/api/v1/rest/datastore/382000000A-000292-002"
    req = requests.get(link)
    data = json.loads(req.text)

    print("read from mongodb")
    df_parking = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()

    print("build new dataframe")   
    df_new = spark.createDataFrame(data["result"]["records"])
    df_new = df_new.withColumn('AVAILABLECAR', regexp_replace('AVAILABLECAR', '-9', 'No Data'))
    df_new = df_new.select(col("ID").alias("park_id"),col("AVAILABLECAR").alias("remain_car"))

    df_final = df_parking.join(df_new, df_parking["park_id"] == df_new["park_id"])

    print("write to db")
    df_final.write.format("com.mongodb.spark.sql").mode("overwrite").save()                                                                                      





                    			