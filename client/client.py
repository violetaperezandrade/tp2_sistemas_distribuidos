import socket
import logging
import csv
import time

from util import protocol
from util.constants import (AIRPORT_REGISTER,
                            FLIGHT_REGISTER,
                            EOF_FLIGHTS_FILE,
                            EOF_AIRPORTS_FILE)

BATCH_SIZE = 1000


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
        self.__close_connection()

    def __read_and_send_lines(self):
        with (open(self.__flights_file, mode='r') as file2,
              open(self.__airports_file, mode='r',
                   encoding='utf-8-sig') as file1):
            reader1 = csv.DictReader(file1, delimiter=";")
            rows = []
            for row in reader1:
                rows.append(row)
                if len(rows) == BATCH_SIZE:
                    try:
                        msg = protocol.encode_registers_batch(rows, AIRPORT_REGISTER)
                        self.__send_msg(msg)
                        self.__retrieve_server_ack()
                        logging.debug(
                            f'action: sent line | result: success | msg: {msg}')
                    except OSError as e:
                        logging.error(
                            f'action: sent line | result: fail | error: {e}')
                    rows = []
            if len(rows) != 0:
                try:
                    msg = protocol.encode_registers_batch(rows, AIRPORT_REGISTER)
                    self.__send_msg(msg)
                    self.__retrieve_server_ack()
                    logging.debug(
                        f'action: sent line | result: success | msg: {msg}')
                except OSError as e:
                    logging.error(
                        f'action: sent line | result: fail | error: {e}')
                rows = []

            self.__send_eof(EOF_AIRPORTS_FILE)
            self.__retrieve_server_ack()
            reader2 = csv.DictReader(file2)
            for row in reader2:
                rows.append(row)
                if len(rows) == BATCH_SIZE:
                    try:
                        msg = protocol.encode_registers_batch(rows, FLIGHT_REGISTER)
                        self.__send_msg(msg)
                        self.__retrieve_server_ack()
                        logging.debug(
                            f'action: sent line | result: success | msg: {msg}')
                    except OSError as e:
                        logging.error(
                            f'action: sent line | result: fail | error: {e}')
                    rows = []
            if len(rows) != 0:
                try:
                    msg = protocol.encode_registers_batch(rows, FLIGHT_REGISTER)
                    self.__send_msg(msg)
                    self.__retrieve_server_ack()
                    logging.debug(
                        f'action: sent line | result: success | msg: {msg}')
                except OSError as e:
                    logging.error(
                        f'action: sent line | result: fail | error: {e}')
                rows = []
        self.__send_eof(EOF_FLIGHTS_FILE)
        self.__retrieve_server_ack()

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
            f"| port: {self._server_address[1]} | result: in_progress")
        self._client_socket.connect(self._server_address)
        logging.info(
            f'action: start_connection | host: {self._server_address[0]} | '
            f'port: {self._server_address[1]} | result: success'
        )

    def __retrieve_server_ack(self):
        msg = self.__read_exact(1)
        ack = protocol.decode_server_ack(msg)
        if ack != 8:
            logging.info(
                f'action: receive_message | host: {self._server_address[0]} | '
                f'port: {self._server_address[1]} | result: error'
                f'|received unkown ack from server: {ack}'
            )
        else:
            logging.info(
                f'action: receive_message | host: {self._server_address[0]} | '
                f'port: {self._server_address[1]} | result: success'
                f'|received ack from server'
            )


    def __send_msg(self, message):
        bytes_sent = 0
        while bytes_sent < len(message):
            chunk_size = self._client_socket.send(message[bytes_sent:])
            logging.debug(
                f'action: sending_message | result: success |'
                f' message: {message} | bytes_sent: {chunk_size}')
            bytes_sent += chunk_size


    def __read_exact(self, bytes_to_read):
        bytes_read = self._client_socket.recv(bytes_to_read)
        while len(bytes_read) < bytes_to_read:
            new_bytes_read = self._client_socket.recv(bytes_to_read - len(bytes_read))
            bytes_read += new_bytes_read
        return bytes_read

    def __close_connection(self):
        """
        Close connection

        Function close TCP client connection with server
        """

        # Connection arrived
        logging.debug('action: close_connection | result: in_progress')
        self._client_socket.close()
        logging.info('action: close_connection | result: success ')
