import datetime
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import FloatType


def calculate_average_velocity(start_time: datetime,
                               end_time: datetime,
                               distance: float):

    return 3600 * distance / ((end_time - start_time).total_seconds())


def calculate_average_velocity_for_each_taxi_type(df: DataFrame):

    average_velocity_udf = F.udf(calculate_average_velocity, FloatType())
    df = df.withColumn(
        "average_velocity",
        average_velocity_udf("pep_pickup_datetime",
                             "pep_dropoff_datetime",
                             "distance"))

    df = (
        df
        .groupBy("taxi_type")
        .agg(F.round(F.mean('average_velocity'), 2).alias("average_velocity"))
    )
    return df


def get_day_of_week_has_min_customer_count(df: DataFrame):

    df = df.where(F.col("customer_count") == 1)
    min_count_all = df.groupBy(["customer_count", "year", "day"]).count()

    min_count_per_year = (
        min_count_all.groupBy(["year"]).agg(F.min("count").alias("count"))
    )
    df = min_count_all.join(
        min_count_per_year,
        on=["year", "count"],
        how="inner").select("year", "day")

    return df


def get_top_3_bussiest_hours(df: DataFrame):

    df = (
        df.groupBy(["hour"]).count()
        .orderBy(F.col("count").desc()).limit(3)
        .select("hour"))

    return df
