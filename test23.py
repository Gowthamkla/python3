from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Df creation").master("local[*]").getOrCreate()

cust_data = [
    {"emp_id":1,"emp_name":"Santa","country":"Cbe"},
    {"emp_id":2,"emp_name":"Fanta","country":"Blr"}
]

df = spark.createDataFrame(cust_data)

print(df.show())