import os
import socket

import pika

from result_handler import ResultHandler


def main():
    listen_backlog = int(os.environ['LISTEN_BACKLOG'])
    total_clients = int(os.getenv("TOTAL_CLIENTS"))
    result_handler = ResultHandler(listen_backlog, total_clients)
    try:
        result_handler.run()
    except pika.exceptions.ChannelWrongStateError:
        pass


if __name__ == '__main__':
    main()
