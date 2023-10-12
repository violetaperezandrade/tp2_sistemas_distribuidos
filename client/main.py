"""Basic RabbitMQ client."""
from configparser import ConfigParser
# pylint: disable=import-error
from sender_client import SenderClient
from listener_client import ListenerClient
import logging
import os
from multiprocessing import Process


def initialize_config():

    config = ConfigParser(os.environ)
    # If config.ini does not exists original config object is not modified
    config.read("config.ini")

    config_params = {}
    try:
        config_params["host"] = config["DEFAULT"]["SERVER_HOST"]
        config_params["port"] = int(config["DEFAULT"]["SERVER_PORT"])
        config_params["host_listen"] = config["DEFAULT"]["LISTEN_HOST"]
        config_params["logging_level"] = config["DEFAULT"]["LOGGING_LEVEL"]
        config_params["flights_name"] = config["DEFAULT"]["FLIGHTS_FILE"]
        config_params["airports_name"] = config["DEFAULT"]["AIRPORTS_FILE"]
        config_params["queries"] = int(config["DEFAULT"]["QUERIES"])
    except KeyError as e:
        raise KeyError(
            "Key was not found. Error: {} .Aborting client".format(e))
    except ValueError as e:
        raise ValueError(
            "Key could not be parsed. Error: {}. Aborting client".format(e))

    return config_params


def run_listener(address, query_number):
    listener_client = ListenerClient(address, query_number)
    listener_client.run()


def main():

    config_params = initialize_config()
    logging_level = config_params["logging_level"]
    port = config_params["port"]
    host = config_params["host"]
    flights_name = config_params["flights_name"]
    airports_name = config_params["airports_name"]
    queries = config_params["queries"]
    host_listen = config_params["host_listen"]

    server_address = (host, port)
    initialize_log(logging_level)
    sender_client = SenderClient(server_address, flights_name, airports_name)
    sender_client.run()

    listener_processes = []
    for i in range(1, queries+1):
        listening_address = (host_listen, i)
        listener_process = Process(target=run_listener,
                                   args=(listening_address, i))
        listener_processes.append(listener_process)
        listener_process.start()

    for listener_process in listener_processes:
        listener_process.join()


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
