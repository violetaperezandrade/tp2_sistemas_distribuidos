"""Basic RabbitMQ client."""

# pylint: disable=import-error
import util.queue_middleware


def main():
    """Send message through queue."""
    rabbitmq_mw = util.queue_middleware.QueueMiddleware()
    rabbitmq_mw.send_message_to('testing_queue', 'Hello world!')
    print('Sent Hello world!')


if __name__ == '__main__':
    main()
