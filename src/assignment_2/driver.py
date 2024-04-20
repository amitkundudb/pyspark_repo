import logging
from src.assignment_2.util import *

logging.basicConfig(level=logging.INFO, format='%(message)s')

spark = spark_session()
credit_card_df = create_df_custom_schema(spark, credit_card_data, credit_card_custom_schema)
credit_card_df.show()

count_partition = get_no_of_partitions(credit_card_df)
logging.info(f"No of Partitions are {count_partition}")

count_increased_partition = increase_partition_by_5(credit_card_df)
logging.info(f"No of Partitions after increasing are {count_increased_partition}")

count_decreased_partition = decrease_partition_by_5(credit_card_df)
logging.info(f"No of Partitions after decreasing are {count_decreased_partition}")


# 6.output should have 2 columns as card_number, masked_card_number(with output of question 2)

spark.udf.register("masked_card_number_udf", masked_card_number_udf)
masked_df = credit_card_df.withColumn("masked_card_number", masked_card_number_udf("card_number"))
masked_df.show()
