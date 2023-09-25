"""Basic RabbitMQ server."""
# pylint: disable=import-error,unused-argument
import util.queue_middleware


def callback(channel, method, properties, body):
    """Print received message."""
    message = body.decode("utf-8")
    print('Received ' + message)


def main():
    """Receive message through queue."""
    rabbitmq_mw = util.queue_middleware.Middleware()
    rabbitmq_mw.create_queue('testing_queue')
    rabbitmq_mw.setup_message_consumption('testing_queue', callback)
    rabbitmq_mw.start_consuming()


if __name__ == '__main__':
    main()
