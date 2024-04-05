from src.assignment_4.util import *

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    spark = SparkSession.builder.appName("Assignment 4").getOrCreate()
    data_path = "C:/Users/AmitKundu/PycharmProjects/pyspark_repo/resource/assignment_4.json"

    logging.info("1. Read JSON file provided in the attachment")
    df = read_json_file(data_path)
    df.show()

    flattend_df = flatten_dataframe(df)
    logging.info("2. Flatten the data frame which is a custom schema")
    flattend_df.show()

    logging.info("3. Record count when flattened and when it's not flattened")
    record_count_before_after_flatten(df, flattend_df)

    differentiate_with_functions(df)

    logging.info("5. Filter id's which are equal to 1001")
    filtered_df = filter_ids(flattend_df, 1001)
    filtered_df.show()

    logging.info("6. Convert the column names from camel case to snake case")
    df_camel_to_snake = convert_columns_to_snake_case(flattend_df)
    df_camel_to_snake.show()

    logging.info("7. Add a new column named load_date with the current date")
    date_column_df = add_load_date_column(flattend_df)
    date_column_df.show()

    logging.info("8. Create 3 new columns as year, month, and day from the load_date column")
    date_month_year_df = create_year_month_day_columns(date_column_df)
    date_month_year_df.show()