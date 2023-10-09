import pika


class QueueMiddleware:

    def __init__(self):
        self.__connection = (pika.BlockingConnection
                             (pika.ConnectionParameters(host='rabbitmq')))
        self.__channel = self.__connection.channel()
        self.__exit = False
        self.__remake = False

    def create_queue(self, queue_name):
        self.__channel.queue_declare(queue=queue_name)

    def __create_exchange(self, exchange_name, type):
        self.__channel.exchange_declare(exchange=exchange_name,
                                        exchange_type=type)

    def __setup_message_consumption(self, queue_name, user_function):
        self.__channel.basic_consume(queue=queue_name,
                                     on_message_callback=lambda channel,
                                     method,
                                     properties,
                                     body:
                                     (user_function(body),
                                      channel.basic_ack
                                      (delivery_tag=method.delivery_tag),
                                      self.__verify_connection_end()))
        self.__channel.start_consuming()

    def __verify_connection_end(self):
        if self.__exit:
            self.__channel.close()
        if self.__remake:
            self.__exit = False
            self.__channel = self.__connection.channel()

    def finish(self, open_new_channel=False):
        self.__exit = True
        self.__remake = open_new_channel

    # Work queue methods
    def listen_on(self, queue_name, user_function):
        self.create_queue(queue_name)
        self.__channel.basic_qos(prefetch_count=1)
        self.__setup_message_consumption(queue_name, user_function)

    def send_message_to(self, queue_name, message):
        self.create_queue(queue_name)
        self.__channel.basic_publish(exchange='',
                                     routing_key=queue_name,
                                     body=message)

    # Publisher/Subscriber methods
    def publish_on(self, exchange_name, message, routing_key):
        """Publish message on specified exchange."""
        self.__create_exchange(exchange_name, 'topic')
        self.__channel.basic_publish(exchange=exchange_name,
                                     routing_key=routing_key,
                                     body=message)

    def subscribe_to(self, exchange, function, routing_key="#", queue=''):
        self.__create_exchange(exchange, 'topic')
        exclusive = (queue == '')
        result = self.__channel.queue_declare(queue=queue,
                                              exclusive=exclusive)
        queue = result.method.queue
        self.__channel.queue_bind(exchange=exchange,
                                  queue=queue,
                                  routing_key=routing_key)
        self.__setup_message_consumption(queue, function)

    def __del__(self):
        self.__connection.close()
