import yaml


def load_config():

    with open(
        "config/pipeline_config.yaml",
        "r"
    ) as file:

        return yaml.safe_load(file)