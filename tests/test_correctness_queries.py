from datetime import datetime
import unittest
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType)

from data_pipeline import correctness_queries
from spark import get_spark_session as spark


class TestCorrectnessQueries(unittest.TestCase):
    """Test class that contains unittests for
    data_pipeline.correctness_queries library"""

    def test_get_day_of_week_has_min_passenger_count(self):
        """Test if
        data_pipeline.correctness_queries.get_day_of_week_has_min_passenger_count
        works properly"""

        test_df_data = [
            (1, "2019", "5", "2"),
            (2, "2019", "2", "3"),
            (3, "2019", "5", "4"),
            (2, "2019", "15", "2"),
            (1, "2019", "20", "1"),
            (1, "2019", "21", "1"),
            (1, "2019", "4", "2"),
            (1, "2019", "5", "3"),
            (1, "2019", "5", "1"),
            (1, "2019", "3", "2"),
            (1, "2019", "6", "3"),
            (4, "2019", "9", "3"),
            (1, "2020", "5", "4"),
            (1, "2020", "5", "6"),
            (1, "2020", "4", "6"),
            (1, "2020", "3", "5"),
            (1, "2020", "6", "4"),
            (1, "2020", "10", "4"),
            (4, "2020", "9", "4"),
            (4, "2018", "12", "6")
        ]
        test_df_schema = StructType([
            StructField("passenger_count", IntegerType(), False),
            StructField("year", StringType(), False),
            StructField("week", StringType(), False),
            StructField("day", StringType(), False)
        ])
        test_df = spark().createDataFrame(
            test_df_data,
            schema=test_df_schema,
        )

        expected_data = [
            ("2019", "3"),
            ("2020", "5")
        ]
        expected_df_schema = StructType([
            StructField("year", StringType(), False),
            StructField("day", StringType(), False)
        ])
        expected_df = spark().createDataFrame(
            expected_data,
            schema=expected_df_schema,
        )
        actual_df = \
            correctness_queries.get_day_of_week_has_min_passenger_count(test_df)

        expected_count = expected_df.count()
        equal_count = actual_df.join(
            expected_df,
            on=["year", "day"],
            how="inner"
        ).count()

        # compare pair details
        self.assertEqual(equal_count, expected_count)

    def test_get_top_3_bussiest_hours(self):
        """Test if
        data_pipeline.correctness_queries.get_top_3_bussiest_hours
        works properly"""
        test_df_data = [
            (1, "2019", "5", "2"),
            (1, "2019", "4", "2"),
            (2, "2019", "2", "3"),
            (3, "2019", "5", "4"),
            (2, "2019", "15", "2"),
            (1, "2019", "20", "12"),
            (3, "2019", "21", "6"),
            (3, "2019", "4", "2"),
            (1, "2020", "5", "3"),
            (2, "2020", "15", "2"),
            (1, "2020", "20", "12"),
            (4, "2020", "23", "12"),
            (1, "2020", "21", "6"),
            (5, "2020", "4", "2"),
            (1, "2020", "5", "3"),
            (1, "2020", "5", "4"),
            (4, "2020", "11", "12"),
        ]
        test_df_schema = StructType([
            StructField("passenger_count", IntegerType(), False),
            StructField("year", StringType(), False),
            StructField("week", StringType(), False),
            StructField("hour", StringType(), False)
        ])
        test_df = spark().createDataFrame(
            test_df_data,
            schema=test_df_schema,
        )

        expected_data = [
            ("2",),
            ("12",),
            ("3",)
        ]
        expected_df_schema = StructType([
            StructField("hour", StringType(), False)
        ])
        expected_df = spark().createDataFrame(
            expected_data,
            schema=expected_df_schema,
        )
        actual_df = \
            correctness_queries.get_top_3_bussiest_hours(test_df)

        expected_count = expected_df.count()
        equal_count = actual_df.join(
            expected_df,
            on=["hour"],
            how="inner"
        ).count()

        # compare pair details
        self.assertEqual(equal_count, expected_count)

    def test_calculate_average_velocity(self):
        """Test if
        data_pipeline.correctness_queries.calculate_average_velocity
        works properly"""
        test_df_data = [
            ("yellow", datetime(2020, 5, 1, 00, 43, 55), datetime(2020, 5, 1, 00, 56, 23), 2.0),
            ("yellow", datetime(2019, 5, 1, 00, 40, 45), datetime(2019, 5, 1, 00, 45, 40), 4.0),
            ("yellow", datetime(2019, 5, 1, 00, 14, 55), datetime(2019, 5, 1, 00, 22, 46), 6.0),
            ("yellow", datetime(2019, 5, 1, 00, 30, 28), datetime(2019, 5, 1, 00, 47, 48), 10.0),
            ("green", datetime(2018, 5, 1, 00, 25, 56), datetime(2018, 5, 1, 00, 45, 7), 2.0),
            ("green", datetime(2018, 5, 1, 00, 4, 50), datetime(2018, 5, 1, 00, 28, 9), 6.0),
            ("green", datetime(2018, 5, 1, 00, 27, 11), datetime(2018, 5, 1, 00, 35, 22), 8.0),
            ("green", datetime(2018, 5, 1, 00, 54, 45), datetime(2018, 5, 1, 1, 00, 6), 8.0),
            ("green", datetime(2018, 5, 1, 15, 24, 45), datetime(2018, 5, 1, 15, 35, 6), 8.0),
        ]
        test_df_schema = ["taxi_type", "pep_pickup_datetime",
                          "pep_dropoff_datetime", "distance"]
        test_df = spark().createDataFrame(
            test_df_data,
            schema=test_df_schema,
        )

        expected_data = [
            ("green", 43.29),
            ("yellow", 34.73)
        ]
        expected_df_schema = StructType([
            StructField("taxi_type", StringType(), False),
            StructField("average_velocity", DoubleType(), False)
        ])
        expected_df = spark().createDataFrame(
            expected_data,
            schema=expected_df_schema,
        )
        actual_df = \
            correctness_queries.calculate_average_velocity_for_each_taxi_type(test_df)

        expected_count = expected_df.count()
        equal_count = actual_df.join(
            expected_df,
            on=["taxi_type", "average_velocity"],
            how="inner"
        ).count()

        # compare pair details
        self.assertEqual(equal_count, expected_count)


if __name__ == "__main__":
    unittest.main()
