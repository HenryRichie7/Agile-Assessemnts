from pyspark.sql import SparkSession
simpleData = [("i-101","p110",23, 1),
("i-102", "p111", 50, 1),
("i-103", "p111", 50, 3),
("i-104", "p112", 75, 1),
("i-105", "p114", 125, 1),
("i-106", "p115", 100, 1),
("i-107", "p115", 100, 1),
("i-108", "p114", 125, 2),
("i-109", "p113", 100, 1),
("i-110", "p111", 50, 2) ]

 

columns= ["invoice no", "product id", "unit_price", "quantity"] 

 

df = spark.createDataFrame(data = simpleData, schema = columns)
print(df.distinct().count())
print(df.select('quantity').distinct().count())