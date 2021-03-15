from etl_base import EtlBase


class EtlYellowTaxi(EtlBase):

    def update_columns(self):
        super().__init__(config_section="green_tripdata")
        self.raw_data = (
             self.raw_data
             .withColumnRenamed("tpep_pickup_datetime", "pep_pickup_datetime")
             .withColumnRenamed("tpep_dropoff_datetime", "pep_dropoff_datetime")
        )

    def apply_logic(self):
        self.update_columns()
        self.partition_columns()
        self.convert_to_boolean()


if __name__ == '__main__':
    instance = EtlYellowTaxi(config_section="yellow_tripdata")
    instance.pre_etl()
