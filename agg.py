import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import approx_count_distinct, collect_list
from pyspark.sql.functions import collect_set, sum, avg, max, countDistinct, count
from pyspark.sql.functions import first, last, kurtosis, min, mean, skewness
from pyspark.sql.functions import stddev, stddev_samp, stddev_pop, sumDistinct
from pyspark.sql.functions import variance, var_samp, var_pop

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

sd = [("John", "Sales", 3000),
      ("Martin", "Sales", 4600),
      ("Robert", "Sales", 4100),
      ("Maria", "Finance", 3000),
      ("Ken", "Sales", 3000),
      ("Scott", "Finance", 3300),
      ("Jerald", "Finance", 3900),
      ("James", "Marketing", 3000),
      ("Karun", "Marketing", 2000),
      ("Syed", "Sales", 4100)
              ]
schema = ["employee_name", "department", "salary"]

df = spark.createDataFrame(data=sd, schema=schema)
df.printSchema()
df.show(truncate=False)

#min
df.select(min("salary")).show(truncate=False)

#max
df.select(max("salary")).show(truncate=False)

#mean (average)
df.select(mean("salary")).show(truncate=False)

#sum
df.select(sum("salary")).show(truncate=False)

#average
print("avg: " + str(df.select(avg("salary")).collect()[0][0]))

#first
df.select(first("salary")).show(truncate=False)

#last
df.select(last("salary")).show(truncate=False)

#count
print("count: "+str(df.select(count("salary")).collect()[0]))

#collecting in list
df.select(collect_list("salary")).show(truncate=False)

#collecting as set
df.select(collect_set("salary")).show(truncate=False)