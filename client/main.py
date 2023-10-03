"""Basic RabbitMQ client."""
from configparser import ConfigParser
from util import protocol
# pylint: disable=import-error
from util.client import Client
import util.csv_reader
import logging
import os

def initialize_config():

    config = ConfigParser(os.environ)
    # If config.ini does not exists original config object is not modified
    config.read("config.ini")

    config_params = {}
    try:
        config_params["host"] = os.getenv('SERVER_HOST', config["DEFAULT"]["SERVER_HOST"])
        config_params["port"] = int(os.getenv('SERVER_PORT', config["DEFAULT"]["SERVER_PORT"]))
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["flights_name"] = os.getenv('FLIGHTS_FILE', config["DEFAULT"]["FLIGHTS_FILE"])
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting client".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting client".format(e))

    return config_params

def main():
    # """Send message through queue."""

    config_params = initialize_config()
    logging_level = config_params["logging_level"]
    port = config_params["port"]
    host = config_params["host"]
    flights_name = config_params["flights_name"]

    server_address = (host, port)

    initialize_log(logging_level)
    client = Client(server_address)
    client.run()

    try:
        flights_reader = (util.csv_reader.CSVReader(flights_name))
        while True:
            client.send_line(flights_reader.next_line(), 1)
    except OSError:
        print("No such file")
    except StopIteration:
        print("Reached EOF")
        client.send_line([], 0)


def initialize_log(logging_level):
    """
    Python custom logging initialization

    Current timestamp is added to be able to identify in docker
    compose logs the date when the log has arrived
    """
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging_level,
        datefmt='%Y-%m-%d %H:%M:%S',
    )


if __name__ == '__main__':
    main()
