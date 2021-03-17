import os
import requests
import logging

from pyspark.sql.utils import AnalysisException
from pyspark.sql import DataFrame, functions as F

from config import get_config, get_table_schema
from helper.read_write import read_data, write_data
from data_pipeline import correctness_queries as query


class EtlBase():

    def __init__(self, table_name: str):
        pipeline_config = get_config()

        self.transformers = [
            self.seperate_date_columns,
            self.convert_to_boolean
        ]

        data_configs = pipeline_config.get("data")
        self.data_retention = data_configs.get("retention_N_months")
        self.output_parquet_dir = data_configs.get("output_parquet_dir")
        self.output_avro_dir = data_configs.get("output_avro_dir")
        # TODO get year and month programitally
        self.current_year = 2018
        self.current_month = 1

        schema_file_name = data_configs.get("schema_file")
        table_schema = get_table_schema(
            schema_file=schema_file_name, table_name=table_name)
        self.taxi_type = table_schema.get("taxi_type")
        self.tlc_dir = table_schema.get("tlc_dir")
        self.local_dir = table_schema.get("local_dir")
        self.file_format = table_schema.get("file_format")
        self.data_year = None

    def run_etl(self):
        self.run_extract()
        self.run_transform_and_load()
     #   self.validate_correctness()

    def run_extract(self):
        """Extracts dataset from TLC."""
        retention_year = int(self.data_retention/12)
        retention_month = self.data_retention % 12

        year = self.current_year - retention_year
        month = self.current_month - retention_month
        if retention_month > self.current_month:
            year = (self.current_year - 1) - retention_year
            month = (self.current_month + 12) - retention_month

        self.raw_file_names_and_year = []
        for _ in range(self.data_retention):
            if month == 12:
                month = 1
                year += 1
            else:
                month += 1
            raw_data_name = (
                self.file_format
                .replace("YEAR", str(year))
                .replace("MONTH",
                         str(month) if month >= 10 else "0" + str(month))
            )
            self.raw_file_names_and_year.append((raw_data_name, year))
            tlc_path = self.tlc_dir + raw_data_name
            local_path = self.local_dir + raw_data_name

            if os.path.exists(local_path):
                logging.warn(f"{local_path} exist.")
            else:
                logging.warn(f"{local_path} does not exist."
                             f"Downloading from: {tlc_path}")
                #myfile = requests.get(tlc_path)
                #with open(local_path, "wb") as local_file:
                #    local_file.write(myfile.content)
                logging.warn(f"{tlc_path} is downloaded.")

        for filename in os.listdir(self.local_dir):
            if filename not in [name_year[0] for name_year in self.raw_file_names_and_year]:
                logging.warn(f"[Not really removing yet] Removing old file: {filename}")

        logging.warn("run_extract completed")

    def run_transform_and_load(self, csv_delimiter: str = ","):
        """Transforms raw dataset to wanted output formats."""
        if not self.raw_file_names_and_year:
            raise NameError("Empty file names. Make sure you run extraction step (run_extract).")
        if not self.transformers:
            raise NameError("Please define list of transformers.")

        # Iterate extracted dataset
        for raw_file_name, year in self.raw_file_names_and_year:
            raw_data_path = self.local_dir + raw_file_name
            self.data_year = year

            logging.info(f"Reading {raw_data_path}.")
            try:
                data = read_data(
                    raw_data_path=raw_data_path,
                    header=True,
                    sep=csv_delimiter
                )
            except AnalysisException as ex:
                logging.critical(f"Csv file not found in path: {raw_data_path}.")
                raise ex

            logging.info("read_data completed")

            for transform_func in self.transformers:
                data = transform_func(data)

            # TODO can "os" do that? something like os.path.join([dir, taxi_type, file_name])?
            parquet_path = self.output_parquet_dir + "taxi_type=" + self.taxi_type
            logging.info(f"Saving output to {parquet_path}.")
            self.load_output(data=data, output_path=parquet_path, file_type="parquet")

            # TODO can "os" do that? something like os.path.join([dir, taxi_type, file_name])?
            # TODO double check if this really saves in avro format.
            avro_path = self.output_avro_dir + "taxi_type=" + self.taxi_type
            logging.info(f"Saving output to {avro_path}.")
            self.load_output(data=data, output_path=avro_path, file_type="avro")

            logging.info(f"transform_and_load applied to {raw_file_name}.")

        logging.info("run_transform_and_load completed")

    def load_output(self, data: DataFrame, file_type: str, output_path: str):
        """Saves data to output directory."""
        write_data(
            df=data,
            output_path=output_path,
            file_type=file_type,
            partition_number=1,
            folder_partition=["year", "month", "day", "hour"])

    def seperate_date_columns(self, data: DataFrame):
        """Transformer: Seperates datetime columns to year, month, day and hour."""
        data = (
            data
            .withColumn("day", F.date_format("pep_pickup_datetime", "u"))
            .withColumn("day_name", F.date_format("pep_pickup_datetime", "E"))
        )
        data = (
            data
            .withColumn("year", F.year(F.col("pep_pickup_datetime")))
            .withColumn("month", F.month(F.col("pep_pickup_datetime")))
            .withColumn("hour", F.hour(F.col("pep_pickup_datetime")))
        )
        return data

    def remove_redundant_year_records(self, data: DataFrame):
        """Filter data according to year"""
        return data.filter(
            F.year(F.col("pep_pickup_datetime")) == self.data_year)

    def convert_to_boolean(self, data: DataFrame):
        """Transformer: Converts store_and_fwd_flag to boolean."""
        data = data.withColumn(
            "store_and_fwd_flag",
            F.when(F.col("store_and_fwd_flag") == "N", False).otherwise(True))
        return data

    def validate_correctness(self):
        """Validate correctness of dataset."""
        df = read_data(
            raw_data_path=self.output_parquet_dir,
            file_type="parquet"
        )
       a = query.get_day_of_week_has_min_passenger_count(df)
       b = query.get_top_3_bussiest_hours(df)
       c = query.calculate_avarage_distance(df)
