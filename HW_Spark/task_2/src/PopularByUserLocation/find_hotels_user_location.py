from pyspark.sql import SparkSession
from pyspark.sql.functions import col

APP_NAME = "Popular Booking Country"

""" Find the most popular country where hotels are booked 
    and searched from the same country """


def find_hotels_user_location(spark):

    source_path = "/user/kl/expedia-hotel-recommendations-parquet/train.parquet/"

    countBkdDF = (spark.read.parquet(source_path)
                  .filter((col("is_booking") == "1"))
                  .groupBy("user_location_country")
                  .count()
                  .select(
                        col("user_location_country"),
                        col("count").alias("cnt_booked")
                            )
                  )

    countSrchdDF = (spark.read.parquet(source_path)
                    .groupBy("user_location_country")
                    .count()
                    .select(
                        col("user_location_country"),
                        col("count").alias("cnt_searched")
                            )
                    )

    resultDF = (countSrchdDF.join(countBkdDF,
                                  countSrchdDF.user_location_country == countBkdDF.user_location_country)
                .withColumn("sum_cnts", (col("cnt_searched") + col("cnt_booked")))
                .orderBy(col("sum_cnts").desc())
                .select(
                        countSrchdDF["user_location_country"],
                        countSrchdDF["cnt_searched"],
                        countBkdDF["cnt_booked"]
                            )
                .limit(1)
                )
    (resultDF
     .coalesce(1)
     .write
     .csv('/user/kl/task_results/task_2', mode='overwrite', header='true'))

    return None


if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName(APP_NAME) \
        .getOrCreate()

    find_hotels_user_location(spark=spark)

    spark.stop()


