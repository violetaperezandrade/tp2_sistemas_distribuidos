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
        config_params["output_queue"] = os.getenv('OUTPUT_QUEUE', config["DEFAULT"]["OUTPUT_QUEUE"])
        config_params["input_queue"] = os.getenv('INPUT_QUEUE', config["DEFAULT"]["INPUT_QUEUE"])
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["column_name"] = os.getenv('COLUMN_NAME', config["DEFAULT"]["COLUMN_NAME"])
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting client".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting client".format(e))

    return config_params


def main():

    config_params = initialize_config()
    input_queue = config_params["input_queue"]
    output_queue = config_params["output_queue"]
    logging_level = config_params["logging_level"]
    column_name = config_params["column_name"]
    
    avg_calculator = AvgCalculator(column_name, output_queue, input_queue)

    connection = connect_mom()
    subscribe_to(connection.channel(), input_queue, avg_calculator.callback,
                 "cleaned_flight_registers")
    connection.close()

if __name__ == '__main__':
    main()
