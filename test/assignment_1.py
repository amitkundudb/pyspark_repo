import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

class TestPysparkFunctions(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Initialize Spark session
        cls.spark = SparkSession.builder.appName("Unit Test").getOrCreate()

        # Define schemas
        cls.purchase_schema = StructType([
            StructField("customer", IntegerType(), True),
            StructField("product_model", StringType(), True)
        ])

        cls.product_schema = StructType([
            StructField("product_model", StringType(), True)
        ])

        # Define sample data
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

        # Create DataFrames
        cls.purchase_data_df = cls.spark.createDataFrame(purchase_data, schema=cls.purchase_schema)
        cls.product_data_df = cls.spark.createDataFrame(product_data, schema=cls.product_schema)

    @classmethod
    def tearDownClass(cls):
        # Stop Spark session
        cls.spark.stop()

    # Test function to find customers who have bought only iphone13
    def test_find_customers_bought_only_iphone13(self):
        iphone13_customers = self.purchase_data_df.filter(col('product_model') == 'iphone13') \
            .select('customer') \
            .distinct().subtract(self.purchase_data_df.filter(col('product_model') != 'iphone13') \
                                 .select('customer').distinct()).collect()
        expected_result = [(3,), (2,)]
        self.assertEqual(sorted(iphone13_customers), sorted(expected_result))

    # Test function to find customers who upgraded from product iphone13 to product iphone14
    def test_find_customers_upgraded_from_iphone13_to_iphone14(self):
        upgraded_customers = self.purchase_data_df.filter(col("product_model") == "iphone13").alias("t1") \
            .join(self.purchase_data_df.filter(col("product_model") == "iphone14").alias("t2"), on=["customer"], how="inner") \
            .filter(col("t1.product_model") == "iphone13") \
            .select(col("t1.customer").alias("customer")) \
            .distinct().collect()
        expected_result = [(3,)]
        self.assertEqual(sorted(upgraded_customers), sorted(expected_result))

    # Test function to find customers who have bought all models in the new Product Data
    def test_find_customers_bought_all_models(self):
        all_prod_customers = self.purchase_data_df.groupBy("customer") \
            .agg(countDistinct("product_model").alias("count_1")) \
            .filter(self.product_data_df.count() == countDistinct("product_model")) \
            .select("customer").collect()
        expected_result = [(1,)]
        self.assertEqual(sorted(all_prod_customers), sorted(expected_result))

if __name__ == '__main__':
    unittest.main()
