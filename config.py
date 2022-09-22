import os
import yaml


def load_yaml(filename):
    if not os.path.exists(filename):
        return {}

    with open(filename, "r") as fd:
        return yaml.load(fd, Loader=yaml.SafeLoader)


def recursive_update(old, new):
    if old is not None and type(old) is dict and type(new) is dict:
        old = old.copy()
        for k, v in new.items():
            old[k] = recursive_update(old.get(k), v)
        return old

    else:
        return new


def LoadSettings():
    config = load_yaml("./config.yml")

    if "secrets" in config:
        secrets = load_yaml(config["secrets"])
        config = recursive_update(config, secrets)

    return config
