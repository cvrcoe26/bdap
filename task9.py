# copy the code into a new notebook in google colab


!pip install pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, avg
spark = SparkSession.builder.appName('Task9').getOrCreate()
df = spark.read.csv('/content/employees.csv', header=True, inferSchema = True)
df.show()

rdd = df.rdd
print(rdd.collect())

print(rdd.count())

df1 = df.withColumn('DOD', to_date(col('HIRE_DATE'), 'dd-MMM-yy'))
df1_filterd = df1.filter(col('DOD') > '2014-12-31')
rdd_filterd = df1_filterd.rdd
print(rdd_filterd.collect())
names_df = rdd.map(lambda x: (x['FIRST_NAME'], x['LAST_NAME']))
print(names_df.collect())
avg_sal = (rdd.map(lambda x: (x['DEPARTMENT_ID'], (x['SALARY'], 1)))
    .reduceByKey(lambda a, b : (a[0]+b[0], a[1]+b[1]))
    .mapValues(lambda x: x[0]/x[1]))

print(avg_sal.collect())
