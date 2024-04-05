from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, datediff, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType


def create_spark_session():
    """
    Function to create a Spark session.
    """
    spark = SparkSession.builder.appName("Assignment 3").getOrCreate()
    return spark


def create_dataframe(spark):
    """
    Function to create a DataFrame with custom schema.
    """
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
    df = spark.createDataFrame(data, schema=schema)
    return df


def rename_column(df, columns):
    """
    Function to rename DataFrame columns.
    """
    for oldname, newname in columns.items():
        df = df.withColumnRenamed(oldname, newname)
    return df


def calculate_actions_last_7_days(df):
    """
    Function to calculate the number of actions performed by each user in the last 7 days.
    """
    df_filtered = df.filter(datediff(expr("date('2023-09-12')"), expr("date(timestamp)")) <= 7)
    actions_performed = df_filtered.groupby("user_id").count()
    return actions_performed


def convert_timestamp_to_date(df):
    df = df.withColumn("login_date", date_format(col("time_stamp"), "yyyy-MM-dd HH:mm:ss").cast(DateType()))
    return df
