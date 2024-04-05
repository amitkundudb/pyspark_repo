import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, explode_outer, posexplode_outer, posexplode, reduce, lit, current_date, \
    year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from functools import reduce


def read_json_file(data_path):
    """
    Function to read JSON file with a specified schema.

    Args:
    - data_path (str): Path to the JSON file.

    Returns:
    - DataFrame: DataFrame containing the JSON data.
    """
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("properties", StructType([
            StructField("name", StringType(), True),
            StructField("storeSize", StringType(), True)
        ]), True),
        StructField("employees", ArrayType(StructType([
            StructField("empId", IntegerType(), True),
            StructField("empName", StringType(), True)
        ])), True)
    ])
    spark = SparkSession.builder.appName("Assignment 3").getOrCreate()
    return spark.read.json(data_path, schema=schema, multiLine=True)

def flatten_dataframe(df):
    """
    Function to flatten the DataFrame.

    Args:
    - df (DataFrame): Input DataFrame.

    Returns:
    - DataFrame: Flattened DataFrame.
    """
    return df.select(col("id"), col("properties.*"), explode("employees").alias("employees")) \
        .select("*", "employees.*").drop("employees")

def record_count_before_after_flatten(df, flattened_df):
    """
    Function to calculate record count before and after flattening.

    Args:
    - df (DataFrame): Original DataFrame.
    - flattened_df (DataFrame): Flattened DataFrame.
    """
    logging.info("record count before flatten %d", df.count())
    logging.info("record count after flatten %d", flattened_df.count())

def differentiate_with_functions(df):
    """
    Function to differentiate the difference using explode, explode_outer, posexplode functions.

    Args:
    - df (DataFrame): Input DataFrame.
    """
    logging.info("Differentiate the difference using explode, explode_outer, posexplode functions")
    df.select(explode("employees").alias("employees")).show()
    df.select(explode_outer("employees").alias("employees")).show()
    df.select(posexplode("employees").alias("pos", "employees")).show()
    df.select(posexplode_outer("employees").alias("pos", "employees")).show()

def filter_ids(df, id_value):
    """
    Function to filter rows with a specific ID.

    Args:
    - df (DataFrame): Input DataFrame.
    - id_value (int): ID value for filtering.

    Returns:
    - DataFrame: Filtered DataFrame.
    """
    return df.filter(col("id") == id_value)

def camel_to_snake(column_name):
    """
    Function to convert camel case column names to snake case.

    Args:
    - column_name (str): Column name in camel case.

    Returns:
    - str: Column name converted to snake case.
    """
    return column_name.replace(" ", "_").lower()

def convert_columns_to_snake_case(df):
    """
    Function to convert all column names from camel case to snake case.

    Args:
    - df (DataFrame): Input DataFrame.

    Returns:
    - DataFrame: DataFrame with column names converted to snake case.
    """
    return reduce(lambda df, col_name: df.withColumnRenamed(col_name, camel_to_snake(col_name)),
                  df.columns, df)

def add_load_date_column(df):
    """
    Function to add a new column named load_date with the current date.

    Args:
    - df (DataFrame): Input DataFrame.

    Returns:
    - DataFrame: DataFrame with a new load_date column.
    """
    return df.withColumn("load_date", lit(current_date()))

def create_year_month_day_columns(df):
    """
    Function to create 3 new columns as year, month, and day from the load_date column.

    Args:
    - df (DataFrame): Input DataFrame.

    Returns:
    - DataFrame: DataFrame with year, month, and day columns.
    """
    return df.withColumn('year', year('load_date')) \
        .withColumn('month', month('load_date')) \
        .withColumn('day', dayofmonth('load_date')).drop("load_date")


