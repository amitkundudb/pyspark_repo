import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from src.assignment_3.util import calculate_actions_last_7_days, convert_timestamp_to_date, rename_column, \
    create_dataframe, create_spark_session


class TestPysparkFunctions(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Initialize Spark session
        cls.spark = SparkSession.builder.appName("Unit Test").getOrCreate()

        # Create DataFrame
        cls.data = [
            (1, 101, 'login', '2023-09-05 08:30:00'),
            (2, 102, 'click', '2023-09-06 12:45:00'),
            (3, 101, 'click', '2023-09-07 14:15:00'),
            (4, 103, 'login', '2023-09-08 09:00:00'),
            (5, 102, 'logout', '2023-09-09 17:30:00'),
            (6, 101, 'click', '2023-09-10 11:20:00'),
            (7, 103, 'click', '2023-09-11 10:15:00'),
            (8, 102, 'click', '2023-09-12 13:10:00')
        ]
        cls.schema = StructType([
            StructField("log id", IntegerType(), True),
            StructField("user$id", IntegerType(), True),
            StructField("action", StringType(), True),
            StructField("timestamp", StringType(), True),
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

    # Test function to rename DataFrame columns
    def test_rename_column(self):
        columns = {"log id": "log_id", "user$id": "user_id", "timestamp": "time_stamp"}
        df = rename_column(self.df, columns)
        self.assertTrue(all(col_name in df.columns for col_name in columns.values()))

    # Test function to calculate the number of actions performed by each user in the last 7 days
    def test_calculate_actions_last_7_days(self):
        result_df = calculate_actions_last_7_days(self.df)
        expected_result = [(101, 3), (102, 3), (103, 2)]
        result_list = [(row['user_id'], row['count']) for row in result_df.collect()]
        self.assertEqual(sorted(result_list), sorted(expected_result))

    # Test function to convert timestamp to date
    def test_convert_timestamp_to_date(self):
        df_with_date = convert_timestamp_to_date(self.df)
        self.assertTrue("login_date" in df_with_date.columns)
        self.assertEqual(df_with_date.select("login_date").first()[0], "2023-09-05")

if __name__ == '__main__':
    unittest.main()
