from util.queue_methods import connect_mom, listen_on
from initial_column_cleaner import ColumnCleaner
from configparser import ConfigParser
import os


def initialize_config():

    config = ConfigParser(os.environ)
    # If config.ini does not exists original config object is not modified
    config.read("config.ini")
    config_params = {}
    try:
        config_params["input_queue"] = os.getenv(
            'INPUT_QUEUE', config["DEFAULT"]["INPUT_QUEUE"])
        config_params["logging_level"] = os.getenv(
            'LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["columns_names"] = os.getenv(
            'COLUMNS_NAMES', config["DEFAULT"]["COLUMNS_NAMES"]).split(",")
        config_params["output_queues"] = os.getenv(
            'OUTPUT_QUEUES', config["DEFAULT"]["OUTPUT_QUEUES"]).split(",")
    except KeyError as e:
        raise KeyError(
            "Key was not found. Error: {} .Aborting client".format(e))
    except ValueError as e:
        raise ValueError(
            "Key could not be parsed. Error: {}. Aborting client".format(e))
    return config_params


def main():
    config_params = initialize_config()
    input_queue = config_params["input_queue"]
    output_queues = config_params["output_queues"]
    columns_names = config_params["columns_names"]
    cleaner = ColumnCleaner(columns_names, output_queues)
    connection = connect_mom()
    listen_on(connection.channel(), input_queue, cleaner.callback)


if __name__ == '__main__':
    main()
