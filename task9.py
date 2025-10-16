# copy the code into a new notebook in google colab


from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Task9').getOrCreate()
df = spark.read.csv('/content/employees.csv', header=True, inferSchema=True)
df.show()
df.count()
from pyspark.sql.functions import to_date, col

# Convert the 'HIRE_DATE' column to a date type
df1 = df.withColumn('DOD', to_date('HIRE_DATE', 'dd-MMM-yy'))
df1.show()
# Filter out employees who joined after 2014
df_filtered = df_date.filter(col('DOD') < '2015-01-01')

# Show the filtered DataFrame
df_filtered.show()
df1.filter(col('DOD') > '2007-06-21').show()
employee_names_RDD = RDD13.map(lambda x: x[0])
print(employee_names_RDD.collect())
from pyspark.sql.functions import col

# Select the 'FIRST_NAME' and 'LAST_NAME' columns to get employee names
employee_names_df = df.select(col("FIRST_NAME"), col("LAST_NAME"))

# Show the DataFrame with employee names
employee_names_df.show()
from pyspark.sql.functions import avg

# Group by 'DEPARTMENT_ID' and calculate the average salary
avg_salary_by_department = df.groupBy("DEPARTMENT_ID").agg(avg("SALARY").alias("average_salary"))

# Show the result
avg_salary_by_department.show()
