"""Basic RabbitMQ server."""
# pylint: disable=import-error,unused-argument
from util.server import Server
import logging


def main():
    # """Receive message through queue."""
    logging_level = "INFO"
    port = 12345

    initialize_log(logging_level)
    server = Server(port, 1)
    server.run()


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
