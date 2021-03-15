from pyspark.sql import SparkSession


def get_spark_session() -> SparkSession:
    """Get or create a spark session."""
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
