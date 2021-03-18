import os
from omegaconf import OmegaConf

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))


def _read_config(config_name: str):
    """Returns requested configuration from same directory."""
    config_path = os.path.join(__location__, config_name)
    return OmegaConf.load(config_path)


def get_config(config_file: str = "config.yaml"):
    """Returns pipeline configuration from same directory."""
    pipeline_config = _read_config(config_name=config_file)
    return pipeline_config.get("pipeline")


def get_table_schema(schema_file: str, table_name: str):
    """Returns data schema from same directory."""
    schema_config = _read_config(config_name=schema_file)
    return schema_config.get("tables").get(table_name)
