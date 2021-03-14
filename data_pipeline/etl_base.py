import logging

from pyspark.sql.utils import AnalysisException
from pyspark.sql import functions as F

from data_pipeline.config import get_config
from helper.read_write import read_data, write_data


class EtlBase():

    raw_data_path = None
    output_path_parquet = None
    output_path_avro = None
    raw_data = None

    def __init__(self, config_section):
        self.config_section = config_section

    def pre_etl(self, csv_delimiter: str = ","):
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
            get_config().get("tables").get(self.config_section)
        )

        self.raw_data_path = script_config.raw_data_path
        self.output_path_parquet = script_config.output_path_parquet
        self.output_path_avro = script_config.output_path_avro

        logging.info(f"reading {self.raw_data_path}")
        try:
            self.raw_data = read_data(
                raw_data_path=self.raw_data_path,
                header=True,
                sep=csv_delimiter
            )

        except AnalysisException as ex:
            raise ex
            logging.critical(f"Csv file not found in path: {self.raw_data_path}.")
            return

        logging.info("read_data completed")

        self.apply_logic()

        logging.info(f"saving output to {self.output_path_parquet} and {self.output_path_avro}")

        self.write(output_path=self.output_path_parquet, file_type="parquet")
        self.write(output_path=self.output_path_avro, file_type="avro")
        logging.info("saved")

    def write(self, file_type: str, output_path: str):
        write_data(
            df=self.raw_data,
            output_path=output_path,
            file_type=file_type,
            partition_number=1,
            folder_partition=["year", "month", "day", "hour"])

    def update_columns():
        """Update_column_names
        """

    def apply_logic(self):
        """
        Partition data
        """

    def partition_columns(self):
        """Seperate date time column to number of week day
        and exact hour. Add these columns to df.

        :param df: input dataframe
        :param df: DataFrame
        :return: df
        rtype: DataFrame
        """
        self.raw_data = (
            self.raw_data
            .withColumn('day', F.date_format('pep_pickup_datetime', 'u'))
            .withColumn('day_name', F.date_format('pep_pickup_datetime', 'E'))
        )
        self.raw_data = (
            self.raw_data
            .withColumn("year", F.year(F.col("pep_pickup_datetime")))
            .withColumn("month", F.month(F.col("pep_pickup_datetime")))
            .withColumn("hour", F.hour(F.col("pep_pickup_datetime")))
        )

    def convert_to_boolean(self):

        self.raw_data = self.raw_data.withColumn(
            "store_and_fwd_flag",
            F.when(F.col("store_and_fwd_flag") == "N", False).otherwise(True))
