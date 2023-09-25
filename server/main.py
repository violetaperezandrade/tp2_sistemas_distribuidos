"""Basic RabbitMQ server."""
# pylint: disable=import-error,unused-argument
import util.queue_middleware


def callback_print(body):
    """Print received message."""
    message = body.decode("utf-8")
    print('Received ' + message)


def main():
    """Receive message through queue."""
    rabbitmq_mw = util.queue_middleware.QueueMiddleware()
    rabbitmq_mw.listen_on('testing_queue', callback_print)


if __name__ == '__main__':
    main()
