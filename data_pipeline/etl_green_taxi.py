from etl_base import EtlBase
from pyspark.sql import DataFrame


class EtlGreenTaxi(EtlBase):

    def __init__(self, table_name: str):
        super().__init__(table_name=table_name)
        self.transformers = [
            self.rename_columns,
            self.remove_redundant_year_records,
            self.seperate_date_columns,
            self.convert_to_boolean,
        ]

    def rename_columns(self, data: DataFrame):
        """Transformer: Rename columns starting with 'l'."""
        data = (
            data
            .withColumnRenamed("lpep_pickup_datetime", "pep_pickup_datetime")
            .withColumnRenamed(
                "lpep_dropoff_datetime", "pep_dropoff_datetime")
        )
        return data


if __name__ == '__main__':
    instance = EtlGreenTaxi(table_name='green_tripdata')
    instance.run_etl()
