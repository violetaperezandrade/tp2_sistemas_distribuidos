"""Basic RabbitMQ client."""

# pylint: disable=import-error
# import util.queue_middleware
from util.client import Client
import logging


def main():
    # """Send message through queue."""
    # rabbitmq_mw = util.queue_middleware.QueueMiddleware()
    # rabbitmq_mw.send_message_to('testing_queue', 'Hello world!')
    # print('Sent Hello world!')

    logging_level = "INFO"
    port = 12345
    host = "localhost"

    server_address = (host, port)

    initialize_log(logging_level)
    client = Client(server_address)
    client.start_client_loop()


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
