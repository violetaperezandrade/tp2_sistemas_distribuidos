import pika


def connect_mom():
    connection = (pika.BlockingConnection
                  (pika.ConnectionParameters(host='rabbitmq')))
    return connection


def create_queue(channel, queue_name):
    channel.queue_declare(queue=queue_name)


def create_fanout_exchange(channel, exchange_name):
    channel.exchange_declare(exchange=exchange_name,
                             exchange_type='fanout')


def acknowledge(channel, method):
    channel.basic_ack(delivery_tag=method.delivery_tag)


def setup_message_consumption(channel, queue_name, user_function):
    channel.basic_consume(queue=queue_name,
                          on_message_callback=user_function)
    channel.start_consuming()


# Work queue methods
def listen_on(channel, queue_name, user_function):
    create_queue(channel, queue_name)
    channel.basic_qos(prefetch_count=1)
    setup_message_consumption(channel, queue_name, user_function)


def send_message_to(channel, queue_name, message):
    create_queue(channel, queue_name)
    channel.basic_publish(exchange='',
                          routing_key=queue_name,
                          body=message)


# Publisher/Subscriber methods
def publish_on(channel, exchange_name, message):
    """Publish message on specified exchange."""
    create_fanout_exchange(channel, exchange_name)
    channel.basic_publish(exchange=exchange_name,
                          routing_key='',
                          body=message)


def subscribe_to(channel, exchange_name, user_function):
    create_fanout_exchange(channel, exchange_name)
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange=exchange_name, queue=queue_name)
    setup_message_consumption(channel, queue_name, user_function)
