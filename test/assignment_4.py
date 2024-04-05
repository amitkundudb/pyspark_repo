import unittest
import logging
from io import StringIO
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import col, explode, explode_outer, posexplode_outer, posexplode, lit, current_date, year, month, dayofmonth
from functools import reduce

from src.assignment_4.util import add_load_date_column, create_year_month_day_columns


class TestPysparkFunctions(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Initialize Spark session
        cls.spark = SparkSession.builder.appName("Unit Test").getOrCreate()

        # Create DataFrame
        cls.data = [
            (1, {"name": "Store 1", "storeSize": "Big"}, [{"empId": 101, "empName": "John Doe"}, {"empId": 102, "empName": "Jane Smith"}]),
            (2, {"name": "Store 2", "storeSize": "Small"}, [{"empId": 103, "empName": "Alice"}, {"empId": 104, "empName": "Bob"}])
        ]
        cls.schema = StructType([
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
        cls.df = cls.spark.createDataFrame(cls.data, schema=cls.schema)

    @classmethod
    def tearDownClass(cls):
        # Stop Spark session
        cls.spark.stop()

    # Test function to read JSON file
    def test_read_json_file(self):
        df = read_json_file("test_data.json")
        self.assertEqual(df.count(), 2)

    # Test function to flatten the DataFrame
    def test_flatten_dataframe(self):
        flattened_df = flatten_dataframe(self.df)
        self.assertEqual(flattened_df.count(), 4)

    # Test function to calculate record count before and after flattening
    def test_record_count_before_after_flatten(self):
        captured_output = StringIO()
        logging.basicConfig(stream=captured_output, level=logging.INFO)
        record_count_before_after_flatten(self.df, flatten_dataframe(self.df))
        self.assertIn("record count before flatten 2", captured_output.getvalue())
        self.assertIn("record count after flatten 4", captured_output.getvalue())

    # Test function to differentiate the difference using explode, explode_outer, posexplode functions
    def test_differentiate_with_functions(self):
        captured_output = StringIO()
        logging.basicConfig(stream=captured_output, level=logging.INFO)
        differentiate_with_functions(self.df)
        expected_output = "Differentiate the difference using explode, explode_outer, posexplode functions"
        self.assertIn(expected_output, captured_output.getvalue())

    # Test function to filter rows with a specific ID
    def test_filter_ids(self):
        filtered_df = filter_ids(self.df, 1)
        self.assertEqual(filtered_df.count(), 1)

    # Test function to convert camel case column names to snake case
    def test_camel_to_snake(self):
        self.assertEqual(camel_to_snake("columnName"), "column_name")

    # Test function to convert all column names from camel case to snake case
    def test_convert_columns_to_snake_case(self):
        df_with_snake_case = convert_columns_to_snake_case(self.df)
        self.assertTrue(all("_" in col_name for col_name in df_with_snake_case.columns))

    # Test function to add a new column named load_date with the current date
    def test_add_load_date_column(self):
        df_with_load_date = add_load_date_column(self.df)
        self.assertTrue("load_date" in df_with_load_date.columns)

    # Test function to create 3 new columns as year, month, and day from the load_date column
    def test_create_year_month_day_columns(self):
        df_with_date_columns = add_load_date_column(self.df)
        df_with_ymd_columns = create_year_month_day_columns(df_with_date_columns)
        self.assertTrue(all(col_name in df_with_ymd_columns.columns for col_name in ["year", "month", "day"]))

if __name__ == '__main__':
    unittest.main()
