import logging

from pyspark.sql.utils import AnalysisException
from pyspark.sql import DataFrame, functions as F

from data_pipeline.config import get_config
from helper.read_write import read_data, write_data


def prep_feed_data(config_section: str,
                   csv_delimiter: str = ","):
    """
    Reads csv file from raw data path write it to output folder
    as parquet and avro file format. raw csv path and output
    paths are read from config

    Following fields are expected to be in config
    - tables.<config_section>.raw_data_path
    - tables.<config_section>.output_path_parquet
    - tables.<config_section>.output_path_avro

    :param config_section: key to part of config that
    function get input-output path info
    :param csv_delimiter: delimiter that used in csv file
    :return: None
    """

    script_config = (
        get_config().get("tables").get(config_section)
    )

    raw_data_path = script_config.raw_data_path
    output_path_parquet = script_config.output_path_parquet
    output_path_avro = script_config.output_path_avro

    logging.info(f"reading {raw_data_path}")
    try:
        raw_data = read_data(
            raw_data_path=raw_data_path, header=True, sep=csv_delimiter
        )

    except AnalysisException as ex:
        raise ex
        logging.critical(f"Csv file not found in path: {raw_data_path}.")
        return

    logging.info("read_data completed")

    raw_data = partition_columns(raw_data)
    raw_data = convert_to_boolean(raw_data)

    logging.info(f"saving output to {output_path_parquet} and {output_path_avro}")
    write_data(
        df=raw_data,
        output_path=output_path_parquet,
        file_type="parquet",
        partition_number=1,
        folder_partition=["year", "month", "day", "hour"])
    write_data(
        df=raw_data,
        output_path=output_path_avro,
        file_type="avro",
        partition_number=1,
        folder_partition=["year", "month", "day", "hour"])
    logging.info("saved")


def partition_columns(df: DataFrame):
    """Seperate date time column to number of week day
    and exact hour. Add these columns to df.

    :param df: input dataframe
    :param df: DataFrame
    :return: df
    rtype: DataFrame
    """
    df = df.select(
        'lpep_pickup_datetime',
        F.date_format('lpep_pickup_datetime', 'u').alias('day'),
        F.date_format('lpep_pickup_datetime', 'E').alias('day_name'))

    df = (
        df
        .withColumn("year", F.year(F.col("lpep_pickup_datetime")))
        .withColumn("month", F.month(F.col("lpep_pickup_datetime")))
        .withColumn("hour", F.hour(F.col("lpep_pickup_datetime")))
    )

    return df


def convert_to_boolean(df: DataFrame):

    return df.withColumn(
        "store_and_fwd_flag",
        F.when(F.col("store_and_fwd_flag") == "N", False).otherwise(True))


if __name__ == "__main__":

    config_section = "green_tripdata"
    prep_feed_data(config_section=config_section)
