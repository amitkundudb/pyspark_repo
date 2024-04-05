from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType


# Function to create Spark session
def create_spark_session():
    return SparkSession.builder.appName("Assignment 2").getOrCreate()


# Function to create DataFrame
def create_dataframe(spark):
    data = [
        ("1234567891234567",),
        ("5678912345671234",),
        ("9123456712345678",),
        ("1234567812341122",),
        ("1234567812341342",)
    ]
    schema = StructType([
        StructField("card_number", StringType(), True)
    ])
    return spark.createDataFrame(data, schema=schema)


# Function to print number of partitions
def print_partitions(df):
    initial_partition = df.rdd.getNumPartitions()
    print("Number of partitions before repartitioning:", initial_partition)


# Function to increase partition size to 5
def increase_partitions(df, initial_partition):
    return df.repartition(initial_partition + 5)


# Function to decrease partition size back to original size
def decrease_partitions(df, initial_partition):
    return df.coalesce(initial_partition)


# Function to mask the card numbers, showing only the last 4 digits
def mask_card_numbers(df):
    def hide(card_number):
        return "*" * (len(card_number) - 4) + card_number[-4:]

    hide_udf = udf(hide, StringType())
    return df.withColumn("masked_card_number", hide_udf(df["card_number"]))
