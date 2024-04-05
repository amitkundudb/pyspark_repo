import sys
import unittest
from io import StringIO

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from src.assignment_2.util import increase_partitions, decrease_partitions, print_partitions, create_dataframe, \
    create_spark_session, mask_card_numbers


class TestPysparkFunctions(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Initialize Spark session
        cls.spark = SparkSession.builder.appName("Unit Test").getOrCreate()

        # Create DataFrame
        cls.data = [
            ("1234567891234567",),
            ("5678912345671234",),
            ("9123456712345678",),
            ("1234567812341122",),
            ("1234567812341342",)
        ]
        cls.schema = StructType([
            StructField("card_number", StringType(), True)
        ])
        cls.df = cls.spark.createDataFrame(cls.data, schema=cls.schema)

    @classmethod
    def tearDownClass(cls):
        # Stop Spark session
        cls.spark.stop()

    # Test function to create Spark session
    def test_create_spark_session(self):
        spark_session = create_spark_session()
        self.assertTrue(isinstance(spark_session, SparkSession))

    # Test function to create DataFrame
    def test_create_dataframe(self):
        df = create_dataframe(self.spark)
        self.assertEqual(df.count(), len(self.data))

    # Test function to print number of partitions
    def test_print_partitions(self):
        initial_partition = self.df.rdd.getNumPartitions()
        captured_output = StringIO()
        sys.stdout = captured_output
        print_partitions(self.df)
        sys.stdout = sys.__stdout__
        self.assertIn("Number of partitions before repartitioning: {}".format(initial_partition), captured_output.getvalue())

    # Test function to increase partition size to 5
    def test_increase_partitions(self):
        initial_partition = self.df.rdd.getNumPartitions()
        new_df = increase_partitions(self.df, initial_partition)
        self.assertEqual(new_df.rdd.getNumPartitions(), initial_partition + 5)

    # Test function to decrease partition size back to original size
    def test_decrease_partitions(self):
        initial_partition = self.df.rdd.getNumPartitions()
        new_df = decrease_partitions(self.df, initial_partition)
        self.assertEqual(new_df.rdd.getNumPartitions(), initial_partition)

    # Test function to mask the card numbers, showing only the last 4 digits
    def test_mask_card_numbers(self):
        df_with_masked_cards = mask_card_numbers(self.df)
        masked_card_numbers = df_with_masked_cards.select("masked_card_number").collect()
        expected_result = [('************4567',),
                           ('************1234',),
                           ('************5678',),
                           ('************1122',),
                           ('************1342',)]
        self.assertEqual(masked_card_numbers, expected_result)

if __name__ == '__main__':
    unittest.main()