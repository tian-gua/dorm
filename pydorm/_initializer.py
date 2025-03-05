import yaml

from ._dorm import dorm


def init(path: str):
    with open(path, 'r') as f:
        config_dict = yaml.load(f, Loader=yaml.FullLoader)
        dorm.init(config_dict)
