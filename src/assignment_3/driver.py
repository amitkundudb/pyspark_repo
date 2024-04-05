from src.assignment_3.util import *

if __name__ == "__main__":
    spark = create_spark_session()
    df = create_dataframe(spark)

    # Display original DataFrame
    print("Original DataFrame:")
    df.show()

    # Rename columns
    column_names = {"log id": "log_id", "user$id": "user_id", "action": "user_activity", "timestamp": "time_stamp"}
    df = rename_column(df, column_names)
    print("DataFrame with Renamed Columns:")
    df.show()

    # Calculate actions performed in last 7 days
    print("Actions performed by each user in the last 7 days:")
    actions_last_7_days = calculate_actions_last_7_days(df)
    actions_last_7_days.show()

    # Convert timestamp to login_date
    df = convert_timestamp_to_date(df)
    print("DataFrame with Timestamp Converted to login_date:")
    df.show()
