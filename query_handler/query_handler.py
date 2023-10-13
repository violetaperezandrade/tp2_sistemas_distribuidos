import json
import signal
import socket

from util.constants import EOF_FLIGHTS_FILE
from util.initialization import initialize_queues
from util.queue_middleware import QueueMiddleware
from util import protocol


class QueryHandler:

    def __init__(self, query_number, eof_max, listen_backlog):
        self._query_handler_socket = socket.socket(socket.AF_INET,
                                                   socket.SOCK_STREAM)
        self._query_handler_socket.bind(('', query_number+12345))
        self._query_handler_socket.listen(listen_backlog)
        self.query_number = query_number
        self.__input_queue = f"output_{query_number}"
        self.__middleware = QueueMiddleware()
        self.__eofs_received = 0
        self.__eof_max = eof_max
        self.__client_socket = None

    def run(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        initialize_queues([self.__input_queue], self.__middleware)
        self.__client_socket = self.__accept_new_connection()

        self.__middleware.listen_on(self.__input_queue, self.__callback)
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
        c, addr = self._query_handler_socket.accept()
        return c

    def __send_exact(self, msg):
        bytes_sent = 0
        while bytes_sent < len(msg):
            chunk_size = self.__client_socket.send(msg[bytes_sent:])
            bytes_sent += chunk_size

    def _handle_sigterm(self, signum, frame):
        print(f"Received sigterm, signum: {signum}, frame: {frame}")
        self.__middleware.handle_sigterm(signum, frame)
        msg = protocol.encode_query_result(7)
        self.__send_exact(msg)
        self.__client_socket.shutdown(socket.SHUT_RDWR)
        self.__client_socket.close()
