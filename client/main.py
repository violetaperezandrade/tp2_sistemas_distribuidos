"""Sample python script."""
# pylint: disable=import-error
import util.queue_middleware


def main():
    """Create queue."""
    rabbitmq_mw = util.queue_middleware.Middleware()
    rabbitmq_mw.create_queue("testing_queue")


if __name__ == '__main__':
    main()
