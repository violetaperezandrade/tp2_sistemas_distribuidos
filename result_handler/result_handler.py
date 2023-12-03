import json
import signal
import socket
import os

from util import protocol
from util.constants import SIGTERM
from util.initialization import initialize_queues
from util.queue_middleware import QueueMiddleware
from util.file_manager import log_batch_to_file
from util.recovery_logging import duplicated_message


class ResultHandler:

    def __init__(self, listen_backlog, total_clients):
        self.__client_sockets = {}
        self.__middleware = QueueMiddleware()
        self._result_handler_socket = socket.socket(socket.AF_INET,
                                                    socket.SOCK_STREAM)
        self._result_handler_socket.bind(('', 12346))
        self._result_handler_socket.listen(listen_backlog)
        self.results = {key: set() for key in range(1, 6)}
        self.total_clients = total_clients
        self._filename = "result_handler/result_handler_logs.txt"

    def run(self):
        # signal.signal(signal.SIGTERM, self._handle_sigterm)
        initialize_queues(["results"], self.__middleware)
        while len(self.__client_sockets) < self.total_clients:
            socket = self.__accept_new_connection()
            client_id = int.from_bytes(self._read_exact(1, socket), "big")
            self.__client_sockets[client_id] = socket
        self.__middleware.listen_on("results", self.__callback)

    def __callback(self, body, method):
        result = json.loads(body)
        message_id = result.get('message_id')
        client_id = result.get('client_id')
        client_id = int(client_id)
        result_id = result.get('result_id', message_id)
        query_number = result.get('query_number')
        if result_id in self.results[client_id]:
            self.__middleware.manual_ack(method)
            return
        if int(query_number) == 3:
            self.parse_query_3_result(result.get('result'),
                                      client_id,
                                      query_number)
            self.__middleware.manual_ack(method)
            return
        if duplicated_message(self._filename, result_id):
            self.__middleware.manual_ack(method)
            return
        self.results[client_id].add(result_id)
        if len(self.results[client_id]) == 1:
            log_batch_to_file(self._filename, self.results[client_id])
            self.results[client_id].clear()
        msg = protocol.encode_query_result(result)
        self.__send_exact(msg, client_id)
        self.__middleware.manual_ack(method)

    def __accept_new_connection(self):
        c, addr = self._result_handler_socket.accept()
        return c

    def __send_exact(self, msg, client_id):
        bytes_sent = 0
        while bytes_sent < len(msg):
            chunk_size = self.__client_sockets[client_id].send(msg[bytes_sent:])
            bytes_sent += chunk_size

    def _read_exact(self, bytes_to_read, socket):
        bytes_read = socket.recv(bytes_to_read)
        while len(bytes_read) < bytes_to_read:
            new_bytes_read = socket.recv(
                bytes_to_read - len(bytes_read))
            bytes_read += new_bytes_read
        return bytes_read

    # def _handle_sigterm(self, signum, frame):
    #     self.__middleware.handle_sigterm(signum, frame)
    #     msg = protocol.encode_signal(SIGTERM)
    #     self.__send_exact(msg)
    #     self.__client_socket.shutdown(socket.SHUT_RDWR)
    #     self.__client_socket.close()

    def parse_query_3_result(self, results, client_id, query_number):
        for route, flights in results.items():
            msg = flights[0]
            msg["query_number"] = query_number
            msg["client_id"] = client_id
            msg = protocol.encode_query_result(msg)
            self.__send_exact(msg, client_id)
