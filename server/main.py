"""Basic RabbitMQ server."""
# pylint: disable=import-error,unused-argument
# import util.queue_middleware
from util.server import Server
import logging


# def callback_print(body):
#     """Print received message."""
#     message = body.decode("utf-8")
#     print('Received ' + message)


def main():
    # """Receive message through queue."""
    # rabbitmq_mw = util.queue_middleware.QueueMiddleware()
    # rabbitmq_mw.listen_on('testing_queue', callback_print)
    logging_level = "INFO"
    port = 12345
    host = "localhost"

    server_address = (host, port)

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
