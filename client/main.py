"""Basic RabbitMQ client."""
# pylint: disable=import-error
from client import Client
import logging


def main():
    # """Send message through queue."""

    logging_level = "INFO"
    port = 12345
    host = "server"

    server_address = (host, port)
    initialize_log(logging_level)
    client = Client(server_address, "itineraries_random_demo.csv")
    client.run()


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
