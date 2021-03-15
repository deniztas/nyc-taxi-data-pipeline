from pyspark.sql import DataFrame, functions as F


def calculate_avarage_distance(df: DataFrame):
    # TODO
    pass


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
