import socket
import logging
import csv
import time

from util import protocol


class Client:
    def __init__(self, address, flights_file):
        # Initialize client socket
        self._client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_address = address
        self.__flights_file = flights_file

    def run(self):
        self.__start_connection_with_server()
        self.__read_and_send_lines(self.__flights_file)

        self.__send_eof()
        self.__close_connection()

    def __read_and_send_lines(self, flights_file):
        with open(flights_file, mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            for row in csv_reader:
                msg = protocol.encode_flight_register(row)
                try:
                    self.__send_msg(msg)
                    logging.debug(
                        f'action: sent line | result: success | msg: {msg}')
                except OSError as e:
                    logging.error(
                        f'action: sent line | result: fail | error: {e}')

    def __send_eof(self):
        msg = protocol.encode_eof_b()
        self.__send_msg(msg)

    def __start_connection_with_server(self):
        """
        Start connection with Server

        Function connect TCP client with server
        """

        # Connection arrived
        logging.info(
            f"action: start_connection | host: {self._server_address[0]}"
            f"| port: {self._server_address[1]} | result: in_progress")
        self._client_socket.connect(self._server_address)
        logging.info(
            f'action: start_connection | host: {self._server_address[0]} | '
            f'port: {self._server_address[1]} | result: success'
        )

    def __send_msg(self, message):
        bytes_sent = 0
        while bytes_sent < len(message):
            time.sleep(0.005)
            chunk_size = self._client_socket.send(message[bytes_sent:])
            logging.debug(
                f'action: sending_message | result: success |'
                f' message: {message} | bytes_sent: {chunk_size}')
            bytes_sent += chunk_size

    def __close_connection(self):
        """
        Close connection

        Function close TCP client connection with server
        """

        # Connection arrived
        logging.debug('action: close_connection | result: in_progress')
        self._client_socket.close()
        logging.info('action: close_connection | result: success ')
