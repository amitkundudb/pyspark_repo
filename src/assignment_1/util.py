from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, StructField, StringType
from pyspark.sql.functions import col, countDistinct, count, when

purchase_schema = StructType([
    StructField("customer", IntegerType(), True),
    StructField("product_model", StringType(), True)
])
purchase_data = [(1, "iphone13"),
                 (1, "dell i5 core"),
                 (2, "iphone13"),
                 (2, "dell i5 core"),
                 (3, "iphone13"),
                 (3, "dell i5 core"),
                 (1, "dell i3 core"),
                 (1, "hp i5 core"),
                 (1, "iphone14"),
                 (3, "iphone14"),
                 (4, "iphone13")]

product_schema = StructType([
    StructField("product_model", StringType(), True)
])
product_data = [("iphone13",), ("dell i5 core",), ("dell i3 core",), ("hp i5 core",), ("iphone14",)]
def create_spark_session():
    spark = SparkSession.builder.appName('Pyspark assignment_1').getOrCreate()
    return spark

# 1.Create DataFrame
def create_dataframe(spark, data, schema):
    df = spark.createDataFrame(data, schema)
    return df

# 2.Find the customers who have bought only iphone13

def find_in_df(df,customer_col,column_name, product_name):
    result_df = df.groupBy(customer_col).agg(
        count(when(col(column_name) == product_name, True)).alias("target_count")).filter(col("target_count") == count("*")).drop("target_count")
    return result_df

# 3.Find customers who upgraded from product iphone13 to product iphone14

def find_iphone14(df):
    only_iphone14 = df.filter(col("product_model") == "iphone14")
    only_iphone13 = df.filter(col("product_model") == "iphone13")
    result = only_iphone13.join(only_iphone14, only_iphone13["customer"] == only_iphone14["customer"], "inner")\
        .drop(only_iphone14.customer, only_iphone14.product_model)
    return result

# 4.Find customers who have bought all models in the new Product Data

def find_bought_all(df1, df2):
    customer_models_count = df1.groupBy("customer").agg(countDistinct("product_model").alias("model_count"))
    all_models = customer_models_count.filter(customer_models_count.model_count == df2.count()).select("customer")
    return all_models