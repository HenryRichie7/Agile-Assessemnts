from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank


simpleData = (("James", "Sales", 3000),
("Michael", "Sales", 4600),
("Robert", "Sales", 4100),
("Maria", "Finance", 3000),
("James", "Sales", 3000),
("Scott", "Finance", 3300),
("Jen", "Finance", 3900),
("Jeff", "Marketing", 3000),
("Kumar", "Marketing", 2000),
("Saif", "Sales", 4100) )

 

columns= ["employee_name", "department", "salary"] 

 

df = spark.createDataFrame(data = simpleData, schema = columns)
windowSpec  = Window.partitionBy("department").orderBy("salary")
df2 = df.withColumn("Dense Rank",dense_rank().over(windowSpec))
df2.sort(df.department.desc(),df.salary.asc()).show()