import unittest
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType)

from data_pipeline import correctness_queries
from spark import get_spark_session as spark


class TestCorrectnessQueries(unittest.TestCase):
    """
    Test class that contains unittests for
    data_pipeline.correctness_queries library
    """

    def test_get_day_of_week_has_min_customer_count(self):
        """
        Test if
        data_pipeline.correctness_queries.get_day_of_week_has_min_customer_count
        works properly
        """

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
            (4, "2020", "9", "4")
        ]
        test_df_schema = StructType([
            StructField("customer_count", IntegerType(), False),
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
            correctness_queries.get_day_of_week_has_min_customer_count(test_df)

        expected_count = expected_df.count()
        equal_count = actual_df.join(
            expected_df,
            on=["year", "day"],
            how="inner"
        ).count()

        # compare pair details
        self.assertEqual(equal_count, expected_count)

    def test_get_top_3_bussiest_hours(self):
        """
        Test if
        data_pipeline.correctness_queries.get_top_3_bussiest_hours
        works properly
        """
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
            StructField("customer_count", IntegerType(), False),
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


if __name__ == "__main__":
    unittest.main()
