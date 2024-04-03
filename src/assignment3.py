from pyspark.sql import SparkSession
from pyspark.sql.functions import date_sub, col, date_format, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

spark = SparkSession.builder.appName("Assignment 3").getOrCreate()

data = [
 (1, 101, 'login', '2023-09-05 08:30:00'),
 (2, 102, 'click', '2023-09-06 12:45:00'),
 (3, 101, 'click', '2023-09-07 14:15:00'),
 (4, 103, 'login', '2023-09-08 09:00:00'),
 (5, 102, 'logout', '2023-09-09 17:30:00'),
 (6, 101, 'click', '2023-09-10 11:20:00'),
 (7, 103, 'click', '2023-09-11 10:15:00'),
 (8, 102, 'click', '2023-09-12 13:10:00')
]
schema = StructType([
    StructField("log id", IntegerType(), True),
    StructField("user$id", IntegerType(), True),
    StructField("action", StringType(), True),
    StructField("timestamp", StringType(), True),
])

# 1. Create a Data Frame with custom schema creation by using Struct Type and Struct Field
df = spark.createDataFrame(data,schema = schema)
df.show()

#2.Column names should be log_id, user_id, user_activity, time_stamp using dynamic function
def rename_column(df,columns):
    for oldname, newname in columns.items():
        df = df.withColumnRenamed(oldname,newname)
    return df
column_names = {"log id": "log_id", "user$id": "user_id", "action": "user_activity", "timestamp": "time_stamp"}
df = rename_column(df,column_names)
df.show()

#3. Write a query to calculate the number of actions performed by each user in the last 7 days
action_last_7_days = df.filter(col("time_stamp") >= date_sub(df["time_stamp"],7)).groupBy("user_id").count()
action_last_7_days.show()

# 4. Convert the time stamp column to the login_date column with YYYY-MM-DD format with date type as its data type
df = df.withColumn("login_date",date_format(col("time_stamp"),"yyyy-MM-dd HH:mm:ss").cast(DateType())).drop("time_stamp")
df.show()