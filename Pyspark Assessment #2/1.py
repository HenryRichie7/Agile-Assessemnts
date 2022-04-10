from pyspark.sql.functions import sum as _sum
from pyspark.sql.functions import hour, desc, to_date, col, year, month, quarter
simpleData = (("i-101","85123A","ABC",150,6,"2021-12-01 08:16:00","c-1001"),
("i-102","85124A","XYZ",110,6,"2021-12-01 09:12:00","c-1002"),
("i-103","85125A","MNO",100,4,"2021-12-01 10:00:00","c-1003"),
("i-104","85126A","VWA",102,5,"2021-12-01 10:31:00","c-1004"),
("i-105","85127A","AAS",100,7,"2021-12-01 10:45:00","c-1005"),
("i-106","85128A","FAS",130,3,"2021-12-01 11:06:00","c-1006"),
("i-107","85129A","AFA",175,6,"2021-12-01 11:15:00","c-1007"),
("i-108","85130A","GAG",150,8,"2021-12-01 11:46:00","c-1008"),
("i-109","85131A","AGG",180,8,"2021-12-01 12:56:00","c-1009"),
("i-110","85132A","KKK",200,1,"2021-12-01 14:36:00","c-1010")) 

columns= ["invoice_no", "product_code", "descr", "unit_price", "quantity", "invoice_date", "customer_id"]

df = spark.createDataFrame(data = simpleData, schema = columns)

# Create a schema from this static dataset using dataframe.
df.printSchema()

# Purchase by customers per hour/Total purchase by hour
df1 = df.groupBy(hour("invoice_date")).agg(_sum(col("quantity")*col("unit_price")).alias("total_purchase"))
df1.show()

# Top 3 customer purchase
df.select('customer_id').orderBy(desc(col("quantity")*col("unit_price"))).show(3)

# Total sales by day
df.groupBy(to_date("invoice_date")).agg(_sum(col("quantity")*col("unit_price"))).alias("total_sales").show()

# Total sales by Year
df.groupBy(year("invoice_date")).agg(_sum(col("quantity")*col("unit_price"))).alias("total_sales").show()

# Total sales per month
df.groupBy(month("invoice_date")).agg(_sum(col("quantity")*col("unit_price"))).alias("total_sales").show()

# Total sales by quarter
df.groupBy(quarter("invoice_date")).agg(_sum(col("quantity")*col("unit_price"))).alias("total_sales").show()

#Sort based on sales
df3 = df.withColumn("Sales",col("quantity")*col("unit_price"))
df3.sort(col("Sales").desc()).show()
