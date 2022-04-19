# Databricks notebook source
import requests
from pyspark.sql.functions import sum as _sum
from pyspark.sql.functions import hour, desc, to_date, col, year, month, expr,to_timestamp

data_url = "https://raw.githubusercontent.com/HenryRichie7/Agile-Assessemnts/main/Pyspark%20Assessment%20%233/tripdetail_json.json"
datas = requests.get(data_url).json()

df = spark.read.json(sc.parallelize([datas]))

#Read the tripdetail.json file with the schema having the below mentioned fields
df.show()

#Convert the tpep_pickup_datetime, tpep_dropoff_datetime both columns into IST from PST and add as a seperate column with _IST suffix
df2 = df.withColumn("tpep_pickup_datetime_IST", df.tpep_pickup_datetime + expr('INTERVAL 12 HOURS 30 minutes'))
df2 = df2.withColumn("tpep_dropoff_datetime_IST", df.tpep_dropoff_datetime + expr('INTERVAL 12 HOURS 30 minutes'))

df2.select("tpep_pickup_datetime","tpep_pickup_datetime_IST","tpep_dropoff_datetime","tpep_dropoff_datetime_IST").show()

#Add addition column TravelTime by finding the difference between tpep_pickup_datetime and tpep_dropoff_datetime dates

df3 = df2.withColumn("TravelTime",(to_timestamp(col("tpep_dropoff_datetime")).cast("long"))- to_timestamp(col("tpep_pickup_datetime")).cast("long"))
df3.show()

# Write the dataframe into csv files
df3.coalesce(1).write.partitionBy('VendorID').mode("overwrite").format("csv").option("header","true").save("dbfs:/FileStore/df/Data_VendorID1")

# Build a temp view on top of the data frame
df3.createOrReplaceTempView("Temp_tab")

# Write spark sql query to aggregate fare_amount based on Vendor Id
df3.groupBy(col("VendorID")).agg(_sum(col("fare_amount"))).show()

# write the output into single CSV file.
df3.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("dbfs:/FileStore/df/AllOutputs1.csv")
