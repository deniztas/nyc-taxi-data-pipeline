from etl_base import EtlBase
from pyspark.sql import DataFrame


class EtlYellowTaxi(EtlBase):

    def __init__(self, table_name: str):
        super().__init__(table_name=table_name)
        self.transformers = [
            self.rename_columns,
            self.seperate_date_columns,
            self.convert_to_boolean,
        ]

    def rename_columns(self, data: DataFrame):
        """Transformer: Rename columns starting with 't'."""
        data = (
            data
            .withColumnRenamed("tpep_pickup_datetime", "pep_pickup_datetime")
            .withColumnRenamed(
                "tpep_dropoff_datetime", "pep_dropoff_datetime")
        )
        return data


if __name__ == '__main__':
    instance = EtlYellowTaxi(table_name='yellow_tripdata')
    instance.run_etl()
