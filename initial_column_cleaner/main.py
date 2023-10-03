from util.queue_middleware import QueueMiddleware
from initial_column_cleaner import ColumnCleaner
from configparser import ConfigParser
import os

def initialize_config():

    config = ConfigParser(os.environ)
    # If config.ini does not exists original config object is not modified
    config.read("config.ini")
    config_params = {}
    try:
        config_params["input_queue"] = os.getenv('INPUT_QUEUE', config["DEFAULT"]["INPUT_QUEUE"])
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["columns"] = int(os.getenv('COLUMNS', config["DEFAULT"]["COLUMNS"]))
        config_params["field_len"] = int(os.getenv('FIELD_LEN', config["DEFAULT"]["FIELD_LEN"]))
        config_params["columns_name"] = os.getenv('COLUMNS_NAME', config["DEFAULT"]["COLUMNS_NAME"]).split(",")
        config_params["indexes_needed"] = os.getenv('INDEXES_NEEDED', config["DEFAULT"]["INDEXES_NEEDED"]).split(",")
        config_params["output_queues"] = os.getenv('OUTPUT_QUEUES', config["DEFAULT"]["OUTPUT_QUEUES"]).split(",")
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting client".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting client".format(e))

    return config_params

def main():

    config_params = initialize_config()
    input_queue = config_params["input_queue"]
    output_queues = config_params["output_queues"]
    logging_level = config_params["logging_level"]
    columns = config_params["columns"]
    field_len = config_params["field_len"]
    columns_name = config_params["columns_name"]
    indexes_needed = config_params["indexes_needed"]
    cleaner = ColumnCleaner(columns, field_len, columns_name, indexes_needed, output_queues)

    rabbitmq_mw = QueueMiddleware()
    for q in output_queues:
        rabbitmq_mw._create_fanout_exchange(q)
    rabbitmq_mw.listen_on(input_queue, cleaner.callback)

if __name__ == '__main__':
    main()