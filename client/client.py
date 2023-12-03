import time
from abc import ABC, abstractmethod
import socket
import logging


class Client(ABC):
    def __init__(self, address):
        self._client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._address = address
        self._sigterm = False

    @abstractmethod
    def run(self):
        pass

    def _start_connection(self):
        """
        Start connection with Server

        Function connect TCP client with server
        """

        # Connection arrived
        logging.info(
            f"action: start_connection | host: {self._address[0]}"
            f"| port: {self._address[1]} | result: in_progress")
        while True:
            try:
                self._client_socket.connect(self._address)
                break
            except OSError:
                time.sleep(1)
        logging.info(
            f'action: start_connection | host: {self._address[0]} | '
            f'port: {self._address[1]} | result: success'
        )

    def _read_exact(self, bytes_to_read):
        bytes_read = self._client_socket.recv(bytes_to_read)
        while len(bytes_read) < bytes_to_read:
            new_bytes_read = self._client_socket.recv(
                bytes_to_read - len(bytes_read))
            bytes_read += new_bytes_read
        return bytes_read

    def _send_exact(self, message):
        bytes_sent = 0
        while bytes_sent < len(message):
            chunk_size = self._client_socket.send(message[bytes_sent:])
            bytes_sent += chunk_size

    def _close_connection(self):
        """
        Close connection

        Function close TCP client connection with server
        """

        # Connection arrived
        logging.debug('action: close_connection | result: in_progress')
        self._client_socket.close()
        logging.info('action: close_connection | result: success ')
