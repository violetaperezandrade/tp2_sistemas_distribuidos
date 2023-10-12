import json
import signal
import socket
import logging

from util.constants import EOF_FLIGHTS_FILE
from util.initialization import initialize_queues
from util.queue_middleware import QueueMiddleware
from util import protocol


class QueryHandler:

    def __init__(self, query_number, eof_max, listen_backlog):
        self._query_handler_socket = socket.socket(socket.AF_INET,
                                                   socket.SOCK_STREAM)
        self._query_handler_socket.bind(('', query_number))
        self._query_handler_socket.listen(listen_backlog)
        self.query_number = query_number
        self.__input_queue = f"output_{query_number}"
        self.__middleware = QueueMiddleware()
        self.__eofs_received = 0
        self.__eof_max = eof_max
        self.__client_socket = None

    def run(self):
        signal.signal(signal.SIGTERM,
                      self.__middleware.handle_sigterm)
        initialize_queues([self.__input_queue], self.__middleware)
        self.__client_socket = self.__accept_new_connection()
        self.__middleware.listen_on(self.__input_queue, self.__callback)
        logging.info(
            'action: done with results | result: success')
        self.__client_socket.close()

    def __callback(self, body):
        result = json.loads(body)
        op_code = result.get("op_code")

        if op_code == EOF_FLIGHTS_FILE:
            self.__eofs_received += 1
            if self.__eofs_received >= self.__eof_max:
                self.__middleware.finish()
            return
        result.pop('op_code', None)
        msg = protocol.encode_query_result(result)
        self.__send_exact(msg)

    def __accept_new_connection(self):
        logging.info('action: query_handler_accept_connections |'
                     'result: in_progress')
        c, addr = self._query_handler_socket.accept()
        logging.info(
            'action: query_handler_accept_connections | result: success'
            f' | ip: {addr[0]}')
        return c

    def __send_exact(self, msg):
        bytes_sent = 0
        while bytes_sent < len(msg):
            chunk_size = self.__client_socket.send(msg[bytes_sent:])
            bytes_sent += chunk_size
