from pyspark.sql import SparkSession
spark: SparkSession = SparkSession.builder .master("local[*]").appName("RDD dataframe").getOrCreate()

if __name__ =="__main__":
    print("Application started")

    data = ([1,2,3,4,5,6,7,8,9,10,11,12])
    rdd = spark.sparkContext.parallelize(data)

    print(type(rdd))
    print(rdd.collect())
    print(rdd.glom().collect())