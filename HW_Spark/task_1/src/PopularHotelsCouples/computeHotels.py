from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

APP_NAME = "Compute Hotel Couples"

""" Find top 3 most popular hotels between couples.
    (treat hotel as composite key of continent,
    country and market). """


def computeHotels(spark):

    source_path = "/user/kl/expedia-hotel-recommendations-parquet/train.parquet/"

    df = (spark.read.parquet(source_path)
          .filter((col("srch_adults_cnt") == "2"))
          .groupBy("hotel_continent", "hotel_country", "hotel_market")
          .count()
          .select("hotel_continent", "hotel_country", "hotel_market", "count")
          .sort(desc('count'))
          .limit(3)
          )

    (df
     .coalesce(1)
     .write
     .csv('/user/kl/task_results/task_1', mode='overwrite', header='true'))

    return None


if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName(APP_NAME) \
        .getOrCreate()

    computeHotels(spark=spark)

    spark.stop()


