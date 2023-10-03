from util.queue_middleware import QueueMiddleware
from filter_by_three_stopovers import FilterByThreeStopovers
from configparser import ConfigParser
import os

def initialize_config():

    config = ConfigParser(os.environ)
    # If config.ini does not exists original config object is not modified
    config.read("config.ini")
    config_params = {}
    try:
        config_params["query_number"] = os.getenv('QUERY_NUMBER', config["DEFAULT"]["QUERY_NUMBER"])
        config_params["output_queue"] = os.getenv('OUTPUT_QUEUE', config["DEFAULT"]["OUTPUT_QUEUE"])
        config_params["input_queue"] = os.getenv('INPUT_QUEUE', config["DEFAULT"]["INPUT_QUEUE"])
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["max_stopovers"] = int(os.getenv('MAX_STOPOVERS', config["DEFAULT"]["MAX_STOPOVERS"]))
        config_params["column_name"] = os.getenv('COLUMN_NAME', config["DEFAULT"]["COLUMN_NAME"])
        config_params["columns_to_filter"] = os.getenv('COLUMNS_TO_FILTER', config["DEFAULT"]["COLUMNS_TO_FILTER"]).split(",")
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting client".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting client".format(e))

    return config_params

def main():

    config_params = initialize_config()
    query_number = config_params["query_number"]
    input_queue = config_params["input_queue"]
    output_queue = config_params["output_queue"]
    logging_level = config_params["logging_level"]
    columns_to_filter = config_params["columns_to_filter"]
    max_stopovers = config_params["max_stopovers"]
    column_name = config_params["column_name"]
    
    filterByStopOvers = FilterByThreeStopovers(column_name, columns_to_filter, max_stopovers, output_queue, query_number)

    rabbitmq_mw = QueueMiddleware()
    rabbitmq_mw.create_queue(output_queue)
    rabbitmq_mw.subscribe_to(input_queue, filterByStopOvers.callback)

if __name__ == '__main__':
    main()