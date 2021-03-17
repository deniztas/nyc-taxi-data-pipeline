
import logging
from typing import Optional, Dict, Any, Union, List
from pyspark.sql import DataFrame

from spark import get_spark_session as spark


def apply_schema(df: DataFrame,
                 schema: Dict[str, Dict[str, Any]]) -> DataFrame:
    """
    Apply schema to a dataframe. Keys of ``schema`` dictionary must be the
    columns of ``df``. Only these keys will be selected from ``df``.

    :param df: dataframe object to apply schema
    :type df: DataFrame
    :param schema: schema information

        schema structure::

            "column_name": {
                "type": "string",
                "alias": "new_column_name",
                "expression": "expression_to_be_applied_to_column",
            }
    :type schema: Dict[str, Dict[str, str]]

    :return: modified dataframe that column names and data types updated
    :rtype: DataFrame
    :raise ValueError: if key of ``schema`` is not a column of ``df``
    """

    select_expr = []
    for name, col_schema in schema.items():
        if name in df.columns:
            col_type = col_schema.get("type", "string")
            if isinstance(col_type, dict):
                col_type = \
                    f"{col_type['name']}<{col_type['json']['elementType']}>"
            alias = col_schema.get("alias", name)
            col_expr = col_schema.get("expression", 'F.col(name)')
            expr = eval(col_expr).cast(col_type).alias(alias)
            select_expr.append(expr)
            logging.info("Apply expr %s and cast %s to %s as %s",
                         col_expr, name, col_type, alias)
        else:
            msg = "{} is in the schema but not in the dataframe.".format(name)
            logging.error(msg)
            raise KeyError(msg)
    df = df.select(select_expr)
    return df


def read_data(
        raw_data_path: str,
        file_type: str = "csv",
        schema: Optional[dict] = None,
        **kwargs) -> DataFrame:
    """
    Read raw data

    Additional named arguments will be passed into the methods of
    :class:`pyspark.sql.DataFrameReader`

    :param raw_data_path: Path of the raw data.
    :type raw_data_path: str
    :param file_type: Type of the data source. Must be ``csv`` or ``parquet``.
    :type file_type: str
    :param schema: schema of dataframe to be read
    :type schema: Optional[dict]
    :return: raw data as pyspark DataFrame
    :rtype: DataFrame
    """
    data_readers = {
        "csv": spark().read.csv,
        "parquet": spark().read.parquet,
        "avro": spark().read.format("avro").load
    }
    try:
        logging.info("Reading raw %s data from %s", file_type, raw_data_path)
        raw_data = data_readers[file_type](raw_data_path, **kwargs)
        if schema:
            raw_data = apply_schema(raw_data, schema)
    except KeyError as exc:
        raise NotImplementedError(
            "File type {} is not implemented".format(file_type)
        ) from exc
    return raw_data


def write_data(
        df: DataFrame,
        output_path: str,
        file_type: str,
        schema: Optional[dict] = None,
        folder_partition: Optional[Union[List[str], str]] = None,
        partition_number: Optional[Union[str, int]] = None) -> None:
    """
    Write given :class:`DataFrame` to givent path
    with ``mode="overwrite"``

    :param df: input DataFrame
    :type df: DataFrame
    :param output_path: path to be written
    :type output_path: str
    :param file_type: type of written file
    :type file_type: str
    :param schema: schema of dataframe to be written
    :type schema: Optional[dict]
    :param folder_partition: folder partition columns
    :type folder_partition: Optional[Union[List[str], str]]
    :param partition_number: number of partitions
    :type partition_number: Optional[Union[str, int]]
    :return: None
    :rtype: None
    """

    logging.info("Writing to %s", output_path)
    if schema:
        df = apply_schema(df, schema)
    if partition_number:
        df = df.repartition(partition_number)
    if folder_partition:
        df.write.format(file_type).partitionBy(folder_partition).save(
            output_path,
            mode="append"
        )
    else:
        df.write.format(file_type).save(output_path, mode="append")
