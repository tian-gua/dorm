import yaml

from ._dorm import dorm, Dorm


def init(path: str) -> Dorm:
    with open(path, 'r') as f:
        config_dict = yaml.load(f, Loader=yaml.FullLoader)
        dorm.init(config_dict)
    return dorm
