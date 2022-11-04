import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

emp = [(1,"Santhosh",-1,"2018","10","M",3000), \
    (2,"Raj",1,"2010","20","M",4000), \
    (3,"Eddy",1,"2010","10","M",1000), \
    (4,"Jack",2,"2005","10","F",2000), \
    (5,"Bala",2,"2010","40","",-1), \
      (6,"Bala",2,"2010","50","",-1) \
  ]
empColumns = ["emp_id","name","secondary_emp_id","year_joined", \
       "emp_dept_id","gender","salary"]

empDF = spark.createDataFrame(data=emp, schema = empColumns)
empDF.printSchema()
empDF.show(truncate=False)
dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)

#inner join
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"inner").show(truncate=False)

#left join
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"left").show(truncate=False)

#left semi join
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"leftsemi").show(truncate=False)

#left anti join
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"leftanti").show(truncate=False)

#right join
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"right").show(truncate=False)

#full join
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"full").show(truncate=False)