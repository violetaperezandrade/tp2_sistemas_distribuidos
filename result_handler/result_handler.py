import json
import signal
import socket
import os

from util import protocol
from util.constants import SIGTERM, CLEANUP
from util.initialization import initialize_queues
from util.queue_middleware import QueueMiddleware


class ResultHandler:

    def __init__(self, listen_backlog, total_clients):
        self.__client_sockets = {}
        self.__middleware = QueueMiddleware()
        self._result_handler_socket = socket.socket(socket.AF_INET,
                                                    socket.SOCK_STREAM)
        self._result_handler_socket.bind(('', 12346))
        self._result_handler_socket.listen(listen_backlog)
        self.results = dict()
        self.total_clients = total_clients

    def run(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        initialize_queues(["results"], self.__middleware)
        while len(self.__client_sockets) < self.total_clients:
            socket = self.__accept_new_connection()
            client_id = int.from_bytes(self._read_exact(1, socket), "big")
            self.results[client_id] = {key: set() for key in range(1, 6)}
            self.__client_sockets[client_id] = socket
        self.__middleware.listen_on("results", self.__callback)

    def __callback(self, body, method):
        result = json.loads(body)
        message_id = result.pop('message_id', None)
        client_id = int(result.pop('client_id'))
        result_id = result.pop('result_id', None)
        query_number = int(result.get('query_number'))
        op_code = result.pop('op_code', None)
        if op_code:
            if op_code == CLEANUP:
                self.results[client_id] = set()
                self.__middleware.manual_ack(method)
                return
        result_key = message_id
        if query_number >= 3:
            result_key = result_id
        if result_key in self.results[client_id][query_number]:
            self.__middleware.manual_ack(method)
            return
        else:
            self.results[client_id][query_number].add(result_key)
        if query_number == 3:
            self.parse_query_3_result(result.get('result'),
                                      client_id,
                                      query_number)
            self.__middleware.manual_ack(method)
            return
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

    def _handle_sigterm(self, signum, frame):
        self.__middleware.handle_sigterm(signum, frame)
        msg = protocol.encode_signal(SIGTERM)
        for id, client_socket in self.__client_sockets.items():
            self.__send_exact(msg, id)
            client_socket.shutdown(socket.SHUT_RDWR)
            client_socket.close()

    def parse_query_3_result(self, results, client_id, query_number):
        # print(f"fot client_id: {client_id}, sending: {results}")
        for route, flights in results.items():
            for flight in flights:
                flight["query_number"] = query_number
                flight = protocol.encode_query_result(flight)
                self.__send_exact(flight, client_id)
