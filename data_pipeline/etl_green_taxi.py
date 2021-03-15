from data_pipeline.etl_base import EtlBase


class EtlGreenTaxi(EtlBase):

    def update_columns(self):
        super().__init__(config_section="green_tripdata")
        self.raw_data = (
             self.raw_data
             .withColumnRenamed("lpep_pickup_datetime", "pep_pickup_datetime")
             .withColumnRenamed("lpep_dropoff_datetime", "pep_dropoff_datetime")
        )

    def apply_logic(self):
        self.update_columns()
        self.partition_columns()
        self.convert_to_boolean()


if __name__ == '__main__':
    instance = EtlGreenTaxi(config_section="green_tripdata")
    instance.pre_etl()
