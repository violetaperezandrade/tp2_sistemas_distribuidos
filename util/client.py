import socket
import logging
import time

from util import protocol

MSG_LEN = 1024


class Client:
    def __init__(self, address):
        # Initialize client socket
        self._client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_address = address

    def run(self):
        # Missing error handling
        self.__start_connection_with_server()

    def send_line(self, line, opcode):
        line = protocol.encode(line, opcode)
        try:
            self.__send_msg(line, len(line))
            logging.info(
                f'action: sent line | result: success | msg: {line}')
        except OSError as e:
            logging.error(
                "action: sent line | result: fail | error: {e}")

    def __start_connection_with_server(self):
        """
        Start connection with Server

        Function connect TCP client with server
        """

        # Connection arrived
        logging.info(
            f"action: starts_connection | host: {self._server_address[0]} | port: {self._server_address[1]} | result: in_progress")
        self._client_socket.connect(self._server_address)
        logging.info(
            f'action: starts_connection | host: {self._server_address[0]} | port: {self._server_address[1]} | result: success ')

    def __close_connection(self):
        """
        Close connection

        Function close TCP client connection with server
        """

        # Connection arrived
        logging.debug('action: close_connection | result: in_progress')
        self._client_socket.close()
        logging.info(f'action: close_connection | result: success ')

    def __send_msg(self, message, length):
        remaining = length
        pos = 0
        while remaining > 0:
            nBytesSent = self._client_socket.send(message[pos:])
            logging.debug(
                f'action: sending_message | result: success | message: {message} | bytes_sent: {nBytesSent}')
            remaining -= nBytesSent
            pos += nBytesSent
