from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Creating DataFrame").master("local[*]").getOrCreate()

customer_data = [
    {"Cust_id":1,"Cust_Name":"Arun","Cust_Country":"Ind"},
    {"Cust_id":2,"Cust_Name":"Gow","Cust_Country":"SriL"},
    {"Cust_id":3,"Cust_Name":"Lal","Cust_Country":"Aus"}

]

df = spark.createDataFrame(customer_data)


df.show()