from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

APP_NAME = "Process Booking Event"

""" Find top 3 hotels where people with children are interested 
    but not booked in the end """


def process_booking_events(spark):

    target_path = "/user/kl/booking_events/result.parquet"
    checkpoint_path = "/user/kl/booking_events/checkpoint"

    json_schema = StructType([
        StructField("date_time", StringType(), True),
        StructField("site_name", IntegerType(), True),
        StructField("posa_continent", IntegerType(), True),
        StructField("user_location_country", IntegerType(), True),
        StructField("user_location_region", IntegerType(), True),
        StructField("user_location_city", IntegerType(), True),
        StructField("orig_destination_distance", DoubleType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("is_mobile", IntegerType(), True),
        StructField("is_package", IntegerType(), True),
        StructField("channel", IntegerType(), True),
        StructField("srch_ci", StringType(), True),
        StructField("srch_co", StringType(), True),
        StructField("srch_adults_cnt", IntegerType(), True),
        StructField("srch_children_cnt", IntegerType(), True),
        StructField("srch_destination_id", IntegerType(), True),
        StructField("srch_destination_type_id", IntegerType(), True),
        StructField("is_booking", IntegerType(), True),
        StructField("cnt", IntegerType(), True),
        StructField("hotel_continent", IntegerType(), True),
        StructField("hotel_country", IntegerType(), True),
        StructField("hotel_market", IntegerType(), True),
        StructField("hotel_cluster", IntegerType(), True)
    ])

    kafkaDF = (spark
               .readStream
               .format("kafka")
               .option("kafka.bootstrap.servers", "localhost:9092")
               .option("subscribe", "booking")
               .option("startingOffsets", "earliest")
               .load()
               .select(col("value").cast("string").alias("json_msg"))
               )

    resultDF = (kafkaDF
                .withColumn("json", from_json(col("json_msg"), json_schema))
                .select("json.*")
                )

    streamingQuery = (resultDF
                      .writeStream
                      .trigger(processingTime='180 seconds')
                      .queryName("booking_kafka")
                      .format("parquet")
                      .option("checkpointLocation", checkpoint_path)
                      .outputMode("append")
                      .start(target_path)
                      .awaitTermination()
                    )

    for s in spark.streams.active:
        if s.name == "streamingQuery":
            s.stop(timeout=10)

    return None


if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName(APP_NAME) \
        .getOrCreate()

    process_booking_events(spark=spark)


