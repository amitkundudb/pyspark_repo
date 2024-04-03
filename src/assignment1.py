from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("Assignment 1").getOrCreate()

purchase_schema = StructType([
    StructField("customer", IntegerType(), True),
    StructField("product_model", StringType(), True)
])

product_schema = StructType([
    StructField("product_model", StringType(), True)
])
# 1.Create DataFrame as purchase_data_df,product_data_df with custom schema with the below data

purchase_data = [
    (1, "iphone13"),
    (1, "dell i5 core"),
    (2, "iphone13"),
    (2, "dell i5 core"),
    (3, "iphone13"),
    (3, "dell i5 core"),
    (1, "dell i3 core"),
    (1, "hp i5 core"),
    (1, "iphone14"),
    (3, "iphone14"),
    (4, "iphone13")
]
product_data = [
    ("iphone13",),
    ("dell i5 core",),
    ("dell i3 core",),
    ("hp i5 core",),
    ("iphone14",)
]

purchase_data_df = spark.createDataFrame(purchase_data, schema=purchase_schema)
product_data_df = spark.createDataFrame(product_data, schema=product_schema)

purchase_data_df.show()
product_data_df.show()

# 2.Find the customers who have bought only iphone13
print("customers who have bought only iphone13")
iphone13_customers = purchase_data_df.filter(col('product_model') == 'iphone13')\
    .select('customer')\
    .distinct().subtract(purchase_data_df.filter(col('product_model') != 'iphone13')\
                         .select('customer').distinct())

iphone13_customers.show()

# 3.Find customers who upgraded from product iphone13 to product iphone14
print("customers who upgraded from product iphone13 to product iphone14")
upgraded_customers = purchase_data_df.filter(col("product_model") == "iphone13").alias("t1")\
    .join(purchase_data_df.filter(col("product_model") == "iphone14").alias("t2"),on = ["customer"], how = "inner")\
    .filter(col("t1.product_model") == "iphone13")\
    .select(col("t1.customer").alias("customer"))\
    .distinct()
upgraded_customers.show()

#4.Find customers who have bought all models in the new Product Data
print("customers who have bought all models")
all_prod_customers = purchase_data_df.groupBy("customer")\
    .agg(countDistinct("product_model").alias("count_1"))\
    .filter(product_data_df.count() == countDistinct("product_model"))\
    .select("customer")
all_prod_customers.show()

