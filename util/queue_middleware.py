"""Provide a middleware layer to interact with RabbitMQ."""
# pylint: disable=too-few-public-methods
import pika


class QueueMiddleware:
    """Wrapper for RabbitMQ methods."""

    def __init__(self):
        """Initialize connection with RabbitMQ server."""
        self._connection = (pika.BlockingConnection
                            (pika.ConnectionParameters(host='rabbitmq')))
        self._channel = self._connection.channel()

    def __create_queue(self, name):
        """Create a queue with specified name."""
        self._channel.queue_declare(queue=name)

    def __setup_message_consumption(self, queue_name, user_function):
        """Set up queue with user function and start consuming."""
        self._channel.basic_consume(queue=queue_name,
                                    on_message_callback=user_function)
        self._channel.start_consuming()

    def _create_fanout_exchange(self, exchange_name):
        """Private. Create fanout exchange with specified name."""
        self._channel.exchange_declare(exchange=exchange_name,
                                       exchange_type='fanout')

    # Work queue methods
    def listen_on(self, queue_name, user_function):
        """Listen on a specific work queue for messages."""
        self.__create_queue(queue_name)
        self._channel.basic_qos(prefetch_count=1)
        self.__setup_message_consumption(queue_name, user_function)

    def send_message_to(self, queue_name, message):
        """Send message through specified queue."""
        self.__create_queue(queue_name)
        self._channel.basic_publish(exchange='',
                                    routing_key=queue_name,
                                    body=message)

    # Publisher/Subscriber methods
    def subscribe_to(self, exchange_name, user_function):
        """Set up queue to start consuming from specified exchange."""
        self.__create_fanout_exchange(exchange_name)
        result = self._channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue
        self._channel.queue_bind(exchange=exchange_name, queue=queue_name)
        self.__setup_message_consumption(queue_name, user_function)

    def publish_on(self, exchange_name, message):
        """Publish message on specified exchange."""
        self.__create_fanout_exchange(exchange_name)
        self._channel.basic_publish(exchange=exchange_name,
                                    routing_key='',
                                    body=message)

    def __del__(self):
        """Close connection properly."""
        if self._connection is not None:
            self._connection.close()
