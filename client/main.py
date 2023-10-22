# pylint: disable=import-error
from configparser import ConfigParser
import logging
import os
import signal
from multiprocessing import Process

from sender_client import SenderClient
from listener_client import ListenerClient


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
        config_params["address_listen"] = int(config["DEFAULT"]["ADDRESS_LISTEN"])
    except KeyError as e:
        raise KeyError(
            "Key was not found. Error: {} .Aborting client".format(e))
    except ValueError as e:
        raise ValueError(
            "Key could not be parsed. Error: {}. Aborting client".format(e))

    return config_params


def run_listener(address):
    listener_client = ListenerClient(address)
    listener_client.run()


def main():
    config_params = initialize_config()
    logging_level = config_params["logging_level"]
    port = config_params["port"]
    host = config_params["host"]
    flights_name = config_params["flights_name"]
    airports_name = config_params["airports_name"]
    address_listen = config_params["address_listen"]
    host_listen = config_params["host_listen"]
    listening_address = (host_listen, address_listen)
    listener_process = Process(target=run_listener,
                               args=(listening_address,))
    listener_process.start()
    server_address = (host, port)
    initialize_log(logging_level)
    sender_client = SenderClient(server_address, flights_name, airports_name)
    sender_client.run()
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
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
