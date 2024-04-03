from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.appName("Assignment 2").getOrCreate()

data =[("1234567891234567",),
 ("5678912345671234",),
 ("9123456712345678",),
 ("1234567812341122",),
 ("1234567812341342",)]

schema = StructType([
    StructField("card_number", StringType(), True)
])

#1.Create a Dataframe as credit_card_df with different read methods
credit_card_df = spark.createDataFrame(data,schema = schema)
credit_card_df.show()

#Reading the file as csv with custom schema
# csv_data = "C:/Users/AmitKundu/PycharmProjects/pyspark_repo/resource/assignment_2_sample.csv"
# csv_df = spark.read.csv(csv_data,schema = schema)
# csv_df.show()

#2. print number of partitions
initial_partition = credit_card_df.rdd.getNumPartitions()
print("Number of partitions before repartitioning:", initial_partition)

#3. Increase the partition size to 5
credit_card_df = credit_card_df.repartition(initial_partition+5)
print("Number of partitions after repartitioning:", credit_card_df.rdd.getNumPartitions())

#4. Decrease the partition size back to its original partition size
credit_card_df = credit_card_df.coalesce(initial_partition)
print("Gettting back to initial partition:", credit_card_df.rdd.getNumPartitions())

#5.Create a UDF to print only the last 4 digits marking the remaining digits as *
def hide(card_number):
    return "*" * (len(card_number)-4)+card_number[-4:]
hided_udf = udf(hide,StringType())
credit_card_df = credit_card_df.withColumn("masked_card_number", hided_udf(credit_card_df["card_number"]))
credit_card_df.show()

