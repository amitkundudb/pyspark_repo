# Importing the required functions from the other Python file
from src.assignment_2.util import *
def main():
    # Create a Spark session
    spark = create_spark_session()

    # Create DataFrame
    credit_card_df = create_dataframe(spark)
    credit_card_df.show()

    # Print number of partitions
    print_partitions(credit_card_df)

    # Increase partition size to 5
    credit_card_df = increase_partitions(credit_card_df, initial_partition=credit_card_df.rdd.getNumPartitions())
    print("Number of partitions after repartitioning:", credit_card_df.rdd.getNumPartitions())

    # Decrease partition size back to original size
    credit_card_df = decrease_partitions(credit_card_df, initial_partition=credit_card_df.rdd.getNumPartitions())
    print("Returning to initial partition:", credit_card_df.rdd.getNumPartitions())

    # Mask the card numbers
    credit_card_df = mask_card_numbers(credit_card_df)
    credit_card_df.show()

if __name__ == "__main__":
    main()
