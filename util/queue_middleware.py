"""Provide a middleware layer to interact with RabbitMQ."""
# pylint: disable=too-few-public-methods
import pika


class Middleware:
    """Provide a way to interact with RabbitMQ."""

    def __init__(self):
        """Initialize connection with RabbitMQ server."""
        self._connection = (pika.BlockingConnection
                            (pika.ConnectionParameters(host='rabbitmq')))

    def create_queue(self, name):
        """Create a queue with specified name."""
        self._connection.channel().queue_declare(queue=name)
