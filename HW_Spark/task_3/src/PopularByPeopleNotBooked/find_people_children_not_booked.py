from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

APP_NAME = "Hotels Searched Not Booked"

""" Find top 3 hotels where people with children are interested 
    but not booked in the end """


def find_hotels_user_location(spark):

    source_path = "/user/kl/expedia-hotel-recommendations-parquet/train.parquet/"

    resultDF = (spark.read.parquet(source_path)
                .filter((col("is_booking") == "0") & (col("srch_children_cnt") > "0") )
                .groupBy("hotel_continent", "hotel_country", "hotel_market")
                .count()
                .select("hotel_continent", "hotel_country", "hotel_market", "count")
                .sort(desc('count'))
                .limit(3)
                )

    (resultDF
     .coalesce(1)
     .write
     .csv('/user/kl/task_results/task_3', mode='overwrite', header='true'))

    return None


if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName(APP_NAME) \
        .getOrCreate()

    find_hotels_user_location(spark=spark)

    spark.stop()


