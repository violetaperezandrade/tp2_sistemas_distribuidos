import pika


class QueueMiddleware:

    def __init__(self):
        self.__connection = (pika.BlockingConnection
                  (pika.ConnectionParameters(host='rabbitmq')))
        self.__channel = self.__connection.channel()
        self.__exit = False

    def __create_queue(self, queue_name):
        self.__channel.queue_declare(queue=queue_name)

    def __create_fanout_exchange(self, exchange_name):
        self.__channel.exchange_declare(exchange=exchange_name,
                                 exchange_type='fanout')

    def __setup_message_consumption(self, queue_name, user_function):
        self.__channel.basic_consume(queue=queue_name,
                              on_message_callback=lambda channel,
                                                         method,
                                                         properties,
                                                         body:
                              (user_function(body),
                               channel.basic_ack(delivery_tag=method.delivery_tag),
                               self.__verify_connection_end()))
        self.__channel.start_consuming()

    def __verify_connection_end(self):
        if self.__exit:
            self.__channel.close()

    def finish(self):
        self.__exit = True


    # Work queue methods
    def listen_on(self, queue_name, user_function):
        self.__create_queue(queue_name)
        self.__channel.basic_qos(prefetch_count=1)
        self.__setup_message_consumption(queue_name, user_function)

    def send_message_to(self, queue_name, message):
        self.__create_queue(queue_name)
        self.__channel.basic_publish(exchange='',
                              routing_key=queue_name,
                              body=message)

    # Publisher/Subscriber methods
    def publish_on(self, exchange_name, message):
        """Publish message on specified exchange."""
        self.__create_fanout_exchange(exchange_name)
        self.__channel.basic_publish(exchange=exchange_name,
                              routing_key='',
                              body=message)

    def subscribe_to(self, exchange_name, user_function, queue_name=''):
        self.__create_fanout_exchange(exchange_name)
        exclusive = (queue_name == '')
        result = self.__channel.queue_declare(queue=queue_name, exclusive=exclusive)
        queue_name = result.method.queue
        self.__channel.queue_bind(exchange=exchange_name, queue=queue_name)
        self.__setup_message_consumption(queue_name, user_function)


    def __del__(self):
        self.__connection.close()