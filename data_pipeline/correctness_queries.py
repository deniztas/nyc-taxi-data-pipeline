import datetime

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import FloatType

from config import get_config
from helper.read_write import read_data, write_data


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


def get_day_of_week_has_min_passenger_count(df: DataFrame):

    df = df.where(F.col("passenger_count") == 1)
    min_count_all = df.groupBy(["year", "day"]).count()

    min_count_per_year = (
        min_count_all.groupBy(["year"]).agg(F.min("count").alias("count"))
    )
    df = (
        min_count_all
        .join(
            min_count_per_year,
            on=["year", "count"],
            how="inner")
        .where((F.col("year") == 2019) | (F.col("year") == 2020))
        .select("year", "day")
    )

    return df


def get_top_3_bussiest_hours(df: DataFrame):

    df = (
        df.groupBy(["hour"]).count()
        .orderBy(F.col("count").desc()).limit(3)
        .select("hour"))

    return df


def validate_correctness(self):
    """Validate correctness of dataset."""
    pipeline_config = get_config()

    data_configs = pipeline_config.get("data").get("output_parquet_dir")
    output_query_min = pipeline_config.get("query") \
        .get("output_query_min_pass_dir")
    output_query_hours = pipeline_config.get("query") \
        .get("output_query_bussiest_hours_dir")
    output_query_avr = pipeline_config.get("query") \
        .get("output_query_avr_dist_dir")

    df = read_data(
        raw_data_path=data_configs,
        file_type="parquet"
    )
    count_df = get_day_of_week_has_min_passenger_count(df)
    hours_df = get_top_3_bussiest_hours(df)
    distance_df = calculate_average_velocity_for_each_taxi_type(df)

    save_query_resuts(count_df, output_query_min)
    save_query_resuts(hours_df, output_query_hours)
    save_query_resuts(distance_df, output_query_avr)


def save_query_resuts(df, path):
    write_data(
        df=df,
        output_path=path,
        file_type="parquet",
        partition_number=1
    )
