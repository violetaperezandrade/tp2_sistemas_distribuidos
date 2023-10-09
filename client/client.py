import socket
import logging
import csv
import time

from util import protocol
from util.constants import (AIRPORT_REGISTER,
                            FLIGHT_REGISTER,
                            EOF_FLIGHTS_FILE,
                            EOF_AIRPORTS_FILE)


class Client:
    def __init__(self, address, flights_file, airports_file):
        # Initialize client socket
        self._client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_address = address
        self.__flights_file = flights_file
        self.__airports_file = airports_file

    def run(self):
        self.__start_connection_with_server()
        self.__read_and_send_lines()
        self.__send_eof(EOF_FLIGHTS_FILE)
        self.__close_connection()

    def __read_and_send_lines(self):
        with (open(self.__flights_file, mode='r') as file2,
              open(self.__airports_file, mode='r',
                   encoding='utf-8-sig') as file1):
            reader1 = csv.DictReader(file1, delimiter=";")
            for row in reader1:
                msg = protocol.encode_register(row, AIRPORT_REGISTER)
                try:
                    self.__send_msg(msg)
                    logging.debug(
                        f'action: sent line | result: success | msg: {msg}')
                except OSError as e:
                    logging.error(
                        f'action: sent line | result: fail | error: {e}')
            self.__send_eof(EOF_AIRPORTS_FILE)
            reader2 = csv.DictReader(file2)
            for row in reader2:
                msg = protocol.encode_register(row, FLIGHT_REGISTER)
                try:
                    self.__send_msg(msg)
                    logging.debug(
                        f'action: sent line | result: success | msg: {msg}')
                except OSError as e:
                    logging.error(
                        f'action: sent line | result: fail | error: {e}')

    def __send_eof(self, opcode):
        msg = protocol.encode_eof_client(opcode)
        self.__send_msg(msg)

    def __start_connection_with_server(self):
        """
        Start connection with Server

        Function connect TCP client with server
        """

        # Connection arrived
        logging.info(
            f"action: start_connection | host: {self._server_address[0]}"
            "| port: {self._server_address[1]} | result: in_progress")
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
