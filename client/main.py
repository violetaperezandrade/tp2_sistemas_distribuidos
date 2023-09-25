"""Basic RabbitMQ client."""

# pylint: disable=import-error
import util.queue_middleware


def main():
    """Send message through queue."""
    rabbitmq_mw = util.queue_middleware.Middleware()
    rabbitmq_mw.create_queue('testing_queue')
    rabbitmq_mw.publish_message('testing_queue', 'Hello world!')
    print('Sent Hello world!')


if __name__ == '__main__':
    main()
