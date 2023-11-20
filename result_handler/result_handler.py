import json
import signal
import socket
import os

from util import protocol
from util.constants import SIGTERM
from util.initialization import initialize_queues
from util.queue_middleware import QueueMiddleware
from util.file_manager import log_to_file


class ResultHandler:

    def __init__(self, listen_backlog, name):
        self.__client_socket = None
        self.__middleware = QueueMiddleware()
        self._result_handler_socket = socket.socket(socket.AF_INET,
                                                    socket.SOCK_STREAM)
        self._result_handler_socket.bind(('', 12346))
        self._result_handler_socket.listen(listen_backlog)
        self._results = set()
        self._filename = "result_handler/" + name + ".txt"

    def run(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        initialize_queues(["results"], self.__middleware)
        self.__client_socket = self.__accept_new_connection()
        self.__middleware.listen_on("results", self.__callback)

    def __callback(self, body, method):
        result = json.loads(body)
        print(result)
        if result.get('message_id') in self._results:
            self.__middleware.manual_ack(method)
            return
        if os.path.exists(self._filename):
            with open(self._filename, 'r') as file:
                for line in file:
                    line.strip("\n")
                    if line == str(result.get('message_id')):
                        self.__middleware.manual_ack(method)
                        return
        self._results.add(result.get('message_id'))
        if len(self._results) == 1:
            for r in self._results:
                log_to_file(self._filename, r)
            self._results.clear()
        msg = protocol.encode_query_result(result)
        self.__send_exact(msg)
        self.__middleware.manual_ack(method)

    def __accept_new_connection(self):
        c, addr = self._result_handler_socket.accept()
        return c

    def __send_exact(self, msg):
        bytes_sent = 0
        while bytes_sent < len(msg):
            chunk_size = self.__client_socket.send(msg[bytes_sent:])
            bytes_sent += chunk_size

    def _handle_sigterm(self, signum, frame):
        self.__middleware.handle_sigterm(signum, frame)
        msg = protocol.encode_signal(SIGTERM)
        self.__send_exact(msg)
        self.__client_socket.shutdown(socket.SHUT_RDWR)
        self.__client_socket.close()
