import json
from filter_by_average import FilterByAverage
from configparser import ConfigParser
import os


def initialize_config():

    config = ConfigParser(os.environ)
    # If config.ini does not exists original config object is not modified
    config.read("config.ini")
    config_params = {}
    try:
        config_params["output_queue"] = os.getenv('OUTPUT_QUEUE', config["DEFAULT"]["OUTPUT_QUEUE"])
        config_params["input_exchange_1"] = os.getenv('INPUT_EXCHANGE_1', config["DEFAULT"]["INPUT_EXCHANGE_1"])
        config_params["input_exchange_2"] = os.getenv('INPUT_EXCHANGE_2', config["DEFAULT"]["INPUT_EXCHANGE_2"])
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting client".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting client".format(e))

    return config_params


def main():

    config_params = initialize_config()
    avg_exchange = config_params["input_exchange_1"]
    cleaner_column_exchange = config_params["input_exchange_2"]
    output_queue = config_params["output_queue"]
    logging_level = config_params["logging_level"]

    filter_by_average = FilterByAverage(output_queue, "cleaned_column_queue", cleaner_column_exchange)

    filter_by_average.run(avg_exchange)

if __name__ == '__main__':
    main()
