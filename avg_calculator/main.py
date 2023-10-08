from util.queue_methods import (connect_mom,
                                subscribe_to,)
import json
from avg_calculator import AvgCalculator
from configparser import ConfigParser
import os

def initialize_config():

    config = ConfigParser(os.environ)
    # If config.ini does not exists original config object is not modified
    config.read("config.ini")
    config_params = {}
    try:
        config_params["output_exchange"] = os.getenv('OUTPUT_EXCHANGE', config["DEFAULT"]["OUTPUT_EXCHANGE"])
        config_params["input_exchange"] = os.getenv('INPUT_EXCHANGE', config["DEFAULT"]["INPUT_EXCHANGE"])
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["column_name"] = os.getenv('COLUMN_NAME', config["DEFAULT"]["COLUMN_NAME"])
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting client".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting client".format(e))

    return config_params


def main():

    config_params = initialize_config()
    input_exchange = config_params["input_exchange"]
    output_exchange = config_params["output_exchange"]
    logging_level = config_params["logging_level"]
    column_name = config_params["column_name"]
    
    avg_calculator = AvgCalculator(column_name, output_exchange, input_exchange, "avg_queue")

    connection = connect_mom()
    subscribe_to(connection.channel(), input_exchange, avg_calculator.callback, "avg_queue")
    connection.close()

if __name__ == '__main__':
    main()
