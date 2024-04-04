import logging
logging.basicConfig(level=logging.INFO, format = '%(message)s')
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, explode_outer, posexplode_outer, posexplode, reduce, lit, current_date, \
    year, month, day, dayofmonth
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from functools import reduce
spark = SparkSession.builder.appName("Assignment 4").getOrCreate()

data = "C:/Users/AmitKundu/PycharmProjects/pyspark_repo/resource/assignment_4.json"

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("properties",StructType([
        StructField("name",StringType(), True),
        StructField("storeSize",StringType(), True)
    ]), True),
    StructField("employees",ArrayType(StructType([
        StructField("empId", IntegerType(), True),
        StructField("empName", StringType(), True)
    ])), True)
])
logging.info("1. Read JSON file provided in the attachment")
df = spark.read.json(data, schema = schema, multiLine=True)
df.show()

flattend_df = df.select(col("id"), col("properties.*"), explode("employees").alias("employees"))
flattend_df = flattend_df.select("*","employees.*").drop("employees")
logging.info("2. Flatten the data frame which is a custom schema")
flattend_df.show()

logging.info("3. Record count when flattened and when it's not flattened")
logging.info("record count before flatten %d", df.count())
logging.info("record count after flatten %d",flattend_df.count())
# Records increased because of "Employee" array into employee_id and employee_name

logging.info("4.Differentiate the difference using explode, explode outer, posexplode functions")
exploded_df = df.select(explode("employees").alias("employees"))
exploded_df.show()
exploded_df2 = df.select(explode_outer("employees").alias("employees"))
exploded_df2.show()
posexploded_df = df.select(posexplode("employees").alias("pos","employees"))
posexploded_df.show()
posexploded_df2 = df.select(posexplode_outer("employees").alias("pos","employees"))
posexploded_df2.show()


filtered_df = flattend_df.filter(col("id") == 1001)
logging.info("5. Filter id's which are equal to 1001")
filtered_df.show()

def camel_to_snake(column_name):
    return column_name.replace(" ","_").lower()
df_camel_to_snake = reduce(lambda df, col_name: df.withColumnRenamed(col_name, camel_to_snake(col_name)),\
    flattend_df.columns, flattend_df)
logging.info("6. convert the column names from camel case to snake case")
df_camel_to_snake.show()

date_column_df = flattend_df.withColumn("load_date", lit(current_date()))
logging.info("7. Add a new column named load_date with the current date")
date_column_df.show()

logging.info("8. create 3 new columns as year, month, and day from the load_date column")
date_month_year_df = date_column_df.withColumn('year',year('load_date'))\
    .withColumn('month',month('load_date'))\
    .withColumn('day',dayofmonth('load_date')).drop("load_date")
date_month_year_df.show()
