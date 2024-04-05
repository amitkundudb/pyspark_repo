import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, current_date
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(message)s")

def create_dataframes():
    spark = SparkSession.builder.appName('spark-assignment').getOrCreate()

    # Define schemas
    employee_schema = StructType([
        StructField("employee_id", IntegerType(), False),
        StructField("employee_name", StringType(), True),
        StructField("department", StringType(), True),
        StructField("State", StringType(), True),
        StructField("salary", IntegerType(), True),
        StructField("Age", IntegerType(), True)
    ])

    department_schema = StructType([
        StructField("dept_id", StringType(), False),
        StructField("dept_name", StringType(), True)
    ])

    country_schema = StructType([
        StructField("country_code", StringType(), True),
        StructField("country_name", StringType(), True)
    ])

    # Create data for data frames
    employee_data = [
        (11, "james", "D101", "ny", 9000, 34),
        (12, "michel", "D101", "ny", 8900, 32),
        (13, "robert", "D102", "ca", 7900, 29),
        (14, "scott", "D103", "ca", 8000, 36),
        (15, "jen", "D102", "ny", 9500, 38),
        (16, "jeff", "D103", "uk", 9100, 35),
        (17, "maria", "D101", "ny", 7900, 40)
    ]

    department_data = [
        ("D101", "sales"),
        ("D102", "finance"),
        ("D103", "marketing"),
        ("D104", "hr"),
        ("D105", "support")
    ]

    country_data = [
        ("ny", "newyork"),
        ("ca", "California"),
        ("uk", "Russia")
    ]

    # Create data frames
    employee_df = spark.createDataFrame(employee_data, employee_schema)
    department_df = spark.createDataFrame(department_data, department_schema)
    country_df = spark.createDataFrame(country_data, country_schema)

    return employee_df, department_df, country_df


def avg_salary_by_department(employee_df):
    avg_salary = employee_df.groupby("department").avg("salary").alias("Average Salary")
    logging.info("Avg salary of each department ")
    avg_salary.show()


def employees_starts_with_m(employee_df, department_df):
    employee_starts_with_m = employee_df.filter(employee_df.employee_name.startswith('m'))
    starts_with_m = employee_starts_with_m.join(department_df,
                                                employee_starts_with_m["department"] == department_df["dept_id"],
                                                "inner") \
        .select(employee_starts_with_m.employee_name, department_df.dept_name)
    logging.info("Employee’s name and department name whose name starts with ‘m’")
    starts_with_m.show()


def add_bonus_column(employee_df):
    employee_bonus_df = employee_df.withColumn("bonus", employee_df.salary * 2)
    logging.info("Create another new column in  employee_df as a bonus by multiplying employee salary *2")
    employee_bonus_df.show()


def reorder_columns(employee_df):
    employee_df = employee_df.select("employee_id", "employee_name", "salary", "State", "Age", "department")
    logging.info("Reordered columns of employee_df:")
    employee_df.show()


def dynamic_join(df1, df2, how):
    return df1.join(df2, df1.department == df2.dept_id, how)


def perform_joins(employee_df, department_df):
    inner_join_df = dynamic_join(employee_df, department_df, "inner")
    logging.info("Result of an inner join when joining employee_df with department_df:")
    inner_join_df.show()
    left_join_df = dynamic_join(employee_df, department_df, "left")
    logging.info("Result of a left join when joining employee_df with department_df:")
    left_join_df.show()
    right_join_df = dynamic_join(employee_df, department_df, "right")
    logging.info("Result of a right join when joining employee_df with department_df:")
    right_join_df.show()


def update_state_to_country_name(employee_df, country_df):
    def update_country_name(dataframe):
        country_dataframe = dataframe.withColumn("State", when(dataframe["State"] == "uk", "United Kingdom")
                                                 .when(dataframe["State"] == "ny", "New York")
                                                 .when(dataframe["State"] == "ca", "Canada"))
        new_df = country_dataframe.withColumnRenamed("State", "country_name")
        return new_df

    country_name_df = update_country_name(employee_df)
    logging.info("Updated State column to country_name in employee_df:")
    country_name_df.show()


def lowercase_columns_and_add_load_date(dataframe):
    def column_to_lower(dataframe):
        for column in dataframe.columns:
            dataframe = dataframe.withColumnRenamed(column, column.lower())
        return dataframe

    lower_case_column_df = column_to_lower(dataframe)
    date_df = lower_case_column_df.withColumn("load_date", current_date())
    logging.info("Columns converted to lowercase and load_date column added:")
    date_df.show()
