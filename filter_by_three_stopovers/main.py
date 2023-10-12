import pika

from filter_by_three_stopovers import FilterByThreeStopovers
from configparser import ConfigParser


def initialize_config():

    config = ConfigParser()
    # If config.ini does not exists original config object is not modified
    config.read("config.ini")
    config_params = {}
    try:
        config_params["query_number"] = config["DEFAULT"]["QUERY_NUMBER"]
        config_params["output_queue"] = config["DEFAULT"]["OUTPUT_QUEUE"]
        config_params["input_queue"] = config["DEFAULT"]["INPUT_QUEUE"]
        config_params["input_exchange"] = config["DEFAULT"]["INPUT_EXCHANGE"]
        config_params["output_exchange"] = config["DEFAULT"]["OUTPUT_EXCHANGE"]
        config_params["logging_level"] = config["DEFAULT"]["LOGGING_LEVEL"]
        config_params["max_stopovers"] = int(config["DEFAULT"]["MAX_STOPOVERS"])
        config_params["column_name"] = config["DEFAULT"]["COLUMN_NAME"]
        config_params["columns_to_filter"] = config["DEFAULT"]["COLUMNS_TO_FILTER"].split(",")
    except KeyError as e:
        raise KeyError(
            "Key was not found. Error: {} .Aborting client".format(e))
    except ValueError as e:
        raise ValueError(
            "Key could not be parsed. Error: {}. Aborting client".format(e))

    return config_params


def main():

    config_params = initialize_config()
    input_exchange = config_params["input_exchange"]
    input_queue = config_params["input_queue"]
    output_queue = config_params["output_queue"]
    columns_to_filter = config_params["columns_to_filter"]
    max_stopovers = config_params["max_stopovers"]
    column_name = config_params["column_name"]
    input_exchange = config_params["input_exchange"]
    output_exchange = config_params["output_exchange"]

    filterByStopOvers = FilterByThreeStopovers(column_name, columns_to_filter,
                                               max_stopovers, output_queue,
                                               input_queue, output_exchange)

    try:
        filterByStopOvers.run(input_exchange)
    except pika.exceptions.ChannelWrongStateError:
        pass


if __name__ == '__main__':
    main()
