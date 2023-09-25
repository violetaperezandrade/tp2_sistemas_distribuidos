"""Provide a middleware layer to interact with RabbitMQ."""
# pylint: disable=too-few-public-methods
import pika


class Middleware:
    """Wrapper for RabbitMQ methods."""

    def __init__(self):
        """Initialize connection with RabbitMQ server."""
        self._connection = (pika.BlockingConnection
                            (pika.ConnectionParameters(host='rabbitmq')))
        self._channel = self._connection.channel()

    def create_queue(self, name):
        """Create a queue with specified name."""
        self._channel.queue_declare(queue=name)

    def publish_message(self, queue_name, message):
        """Send message through specified queue."""
        self._channel.basic_publish(exchange='',
                                    routing_key=queue_name,
                                    body=message)

    def setup_message_consumption(self, queue_name, callback):
        """Set up queue with specified callback function."""
        self._channel.basic_consume(queue=queue_name,
                                    auto_ack=True,
                                    on_message_callback=callback)

    def start_consuming(self):
        """Start consuming messages on previously setup queue."""
        self._channel.start_consuming()

    def __del__(self):
        """Close connection properly."""
        if self._connection is not None:
            self._connection.close()
