import os
import pika

from result_handler import ResultHandler


def main():
    listen_backlog = int(os.environ['LISTEN_BACKLOG'])
    name = os.getenv("HOSTNAME")
    result_handler = ResultHandler(listen_backlog, name)
    try:
        result_handler.run()
    except pika.exceptions.ChannelWrongStateError:
        pass


if __name__ == '__main__':
    main()
