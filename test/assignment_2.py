
import unittest
from src.assignment_2.util import *


class TestAssignment2(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("Assignment 2 Testcase").getOrCreate()

    def test_create_df(self):
        test_credit_card_data = [("1234567891234567",),
                                 ("5678912345671234",),
                                 ("9123456712345678",),
                                 ("1234567812341122",),
                                 ("1234567812341342",)]
        test_credit_card_custom_schema = StructType([
            StructField("card_number", StringType(), True)
        ])
        df = create_df(self.spark, test_credit_card_data, test_credit_card_custom_schema)
        self.assertEqual(df.count(), len(test_credit_card_data))

    def test_create_df_csv(self):
        path_csv = r"C:\Users\AmitKundu\PycharmProjects\pyspark_repo\resource\assignment_2_sample.csv"
        credit_schema = StructType([
            StructField("card_number", StringType(), True)
        ])
        df = create_df_csv(self.spark, path_csv, credit_schema)
        test_credit_card_data = [("1234567891234560",),
                                 ("5678912345671230",),
                                 ("9123456712345670",),
                                 ("1234567812341120",),
                                 ("1234567812341340",)]
        test_credit_card_custom_schema = StructType([
            StructField("card_number", StringType(), True)
        ])
        expected_df = create_df(self.spark, test_credit_card_data, test_credit_card_custom_schema)
        self.assertEqual(df.collect(), expected_df.collect())

    def test_create_df_json(self):
        path_json = "C:/Users/AmitKundu/PycharmProjects/pyspark_repo/resource/assignment_2_sample.json"
        df = create_df_json(self.spark, path_json)
        test_credit_card_data = [('1234567891234560',),
                                 ('5678912345671230',),
                                 ('9123456712345670',),
                                 ('1234567812341120',),
                                 ('1234567812341340',)]
        test_credit_card_custom_schema = StructType([
            StructField("card_number", StringType(), True)
        ])
        expected_df = create_df(self.spark, test_credit_card_data, test_credit_card_custom_schema)
        self.assertIsNotNone(df.collect(), expected_df.collect())

    def test_get_no_of_partitions(self):
        df = create_df(self.spark, credit_card_data, credit_card_schema)
        partitions = get_no_of_partitions(df)
        self.assertEqual(partitions, 8)

    def test_increase_partition_by_5(self):
        df = create_df(self.spark, credit_card_data, credit_card_schema)
        initial_partitions = get_no_of_partitions(df)
        new_partitions = increase_partition_by_5(df)
        self.assertEqual(new_partitions, initial_partitions + 5)

    def test_decrease_partition_by_5(self):
        df = create_df(self.spark, credit_card_data, credit_card_schema)
        final_partitions = decrease_partition_by_5(df)
        self.assertEqual(final_partitions, 8)

    def test_masked_card_number_udf(self):
        df = create_df_custom_schema(self.spark, credit_card_data, credit_card_custom_schema)
        df = df.withColumn("masked_number", masked_card_number_udf(df["card_number"]))
        masked_numbers = df.select("masked_number").rdd.flatMap(lambda x: x).collect()
        expected_masked_numbers = ['************4567', '************1234', '************5678',
                                   '************1122', '************1342']
        self.assertEqual(masked_numbers, expected_masked_numbers)


if __name__ == '__main__':
    unittest.main()
