import unittest
from io import StringIO
from src.assignment_5.util import *

class TestPysparkFunctions(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Initialize Spark session
        cls.spark = SparkSession.builder.appName("Unit Test").getOrCreate()

        # Create dataframes
        cls.employee_df, cls.department_df, cls.country_df = create_dataframes()

    @classmethod
    def tearDownClass(cls):
        # Stop Spark session
        cls.spark.stop()

    # Test function to calculate average salary by department
    def test_avg_salary_by_department(self):
        captured_output = StringIO()
        logging.basicConfig(stream=captured_output, level=logging.INFO)
        avg_salary_by_department(self.employee_df)
        self.assertIn("Avg salary of each department", captured_output.getvalue())

    # Test function to get employees whose names start with 'm'
    def test_employees_starts_with_m(self):
        captured_output = StringIO()
        logging.basicConfig(stream=captured_output, level=logging.INFO)
        employees_starts_with_m(self.employee_df, self.department_df)
        self.assertIn("Employee’s name and department name whose name starts with ‘m’", captured_output.getvalue())

    # Test function to add bonus column
    def test_add_bonus_column(self):
        captured_output = StringIO()
        logging.basicConfig(stream=captured_output, level=logging.INFO)
        add_bonus_column(self.employee_df)
        self.assertIn("Create another new column in employee_df as a bonus", captured_output.getvalue())

    # Test function to reorder columns
    def test_reorder_columns(self):
        captured_output = StringIO()
        logging.basicConfig(stream=captured_output, level=logging.INFO)
        reorder_columns(self.employee_df)
        self.assertIn("Reordered columns of employee_df:", captured_output.getvalue())

    # Test function to perform inner, left, and right joins
    def test_perform_joins(self):
        captured_output = StringIO()
        logging.basicConfig(stream=captured_output, level=logging.INFO)
        perform_joins(self.employee_df, self.department_df)
        self.assertIn("Result of an inner join when joining employee_df with department_df:", captured_output.getvalue())
        self.assertIn("Result of a left join when joining employee_df with department_df:", captured_output.getvalue())
        self.assertIn("Result of a right join when joining employee_df with department_df:", captured_output.getvalue())

    # Test function to update state to country name
    def test_update_state_to_country_name(self):
        captured_output = StringIO()
        logging.basicConfig(stream=captured_output, level=logging.INFO)
        update_state_to_country_name(self.employee_df, self.country_df)
        self.assertIn("Updated State column to country_name in employee_df:", captured_output.getvalue())

    # Test function to lowercase columns and add load date
    def test_lowercase_columns_and_add_load_date(self):
        captured_output = StringIO()
        logging.basicConfig(stream=captured_output, level=logging.INFO)
        lowercase_columns_and_add_load_date(self.employee_df)
        self.assertIn("Columns converted to lowercase and load_date column added:", captured_output.getvalue())

if __name__ == '__main__':
    unittest.main()
