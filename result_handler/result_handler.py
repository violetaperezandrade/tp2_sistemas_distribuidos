import json
import socket

from util import protocol
from util.constants import EOF_FLIGHTS_FILE, SIGTERM
from util.initialization import initialize_queues
from util.queue_middleware import QueueMiddleware


class ResultHandler:

    def __init__(self, listen_backlog):
        self.__eof_max = 5
        self.__eofs_received = 0
        self.__client_socket = None
        self.__middleware = QueueMiddleware()
        self._result_handler_socket = socket.socket(socket.AF_INET,
                                                    socket.SOCK_STREAM)
        self._result_handler_socket.bind(('', 12346))
        self._result_handler_socket.listen(listen_backlog)

    def run(self):
        initialize_queues(["results"], self.__middleware)
        self.__client_socket = self.__accept_new_connection()

    def __callback(self, body):
        result = json.loads(body)
        op_code = result.get("op_code")
        if op_code == EOF_FLIGHTS_FILE:
            self.__eofs_received += 1
            if self.__eofs_received == self.__eof_max:
                self.__middleware.finish()
            return
        msg = protocol.encode_query_result(result)
        self.__send_exact(msg)

    def __accept_new_connection(self):
        c, addr = self._result_handler_socket.accept()
        return c

    def __send_exact(self, msg):
        bytes_sent = 0
        while bytes_sent < len(msg):
            chunk_size = self.__client_socket.send(msg[bytes_sent:])
            bytes_sent += chunk_size

    def _handle_sigterm(self, signum, frame):
        print("Received SIGTERM")
        self.__middleware.handle_sigterm(signum, frame)
        msg = protocol.encode_signal(SIGTERM)
        self.__send_exact(msg)
        print(f"SENT: {msg}")
        self.__client_socket.shutdown(socket.SHUT_RDWR)
        self.__client_socket.close()
