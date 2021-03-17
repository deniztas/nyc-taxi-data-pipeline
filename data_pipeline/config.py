import os
from omegaconf import OmegaConf


def get_config():
    config_path = "config.yaml"
    defaults_path = os.path.join(os.path.dirname(__file__), config_path)
    defaults_config = OmegaConf.load(defaults_path)
    return defaults_config
