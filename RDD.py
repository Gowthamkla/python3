from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Creation of RDD").master("local[*]").getOrCreate()

data = [1,2,3,4,5]
rdd = spark.sparkContext.parallelize(data)

print(rdd.collect())