import socket
import logging
import csv
import signal

from client import Client
from util import protocol
from util.constants import (AIRPORT_REGISTER,
                            FLIGHT_REGISTER,
                            EOF_FLIGHTS_FILE,
                            EOF_AIRPORTS_FILE)

BATCH_SIZE = 1000

class SenderClient(Client):
    def __init__(self, address, flights_file, airports_file):
        super().__init__(address)  # Call the constructor of the abstract class
        self._flights_file = flights_file
        self._airports_file = airports_file

    def run(self):
        signal.signal(signal.SIGTERM, self.handle_sigterm)
        self._start_connection()
        while not self._sigterm:
            self.__read_and_send_lines()
        if self._sigterm:
            sigterm_msg = protocol.encode_eof_client(9)
            self._send_exact(sigterm_msg)
            self._client_socket.shutdown(socket.SHUT_RDWR)
            logging.info('action: close_client | result: success')
        self._close_connection()

    def handle_sigterm(self, signum, frame):
        logging.info(
            f'action: sigterm received | signum: {signum}, frame:{frame}')
        self._sigterm = True
        return

    def __retrieve_server_ack(self):
        msg = self._read_exact(1)
        ack = protocol.decode_server_ack(msg)
        if ack != 8:
            logging.info(
                f'action: receive_message | host: {self._address[0]} | '
                f'port: {self._address[1]} | result: error'
                f'|received unkown ack from server: {ack}'
            )
        else:
            logging.info(
                f'action: receive_message | host: {self._address[0]} | '
                f'port: {self._address[1]} | result: success'
                f'|received ack from server'
            )

    def __send_eof(self, opcode):
        msg = protocol.encode_eof_client(opcode)
        self._send_exact(msg)

    def __read_and_send_lines(self):
        with (open(self._flights_file, mode='r') as file2,
              open(self._airports_file, mode='r',
                   encoding='utf-8-sig') as file1):
            reader1 = csv.DictReader(file1, delimiter=";")
            rows = []
            for row in reader1:
                if self._sigterm:
                    return
                rows.append(row)
                if len(rows) == BATCH_SIZE:
                    try:
                        msg = protocol.encode_registers_batch(rows, AIRPORT_REGISTER)
                        self._send_exact(msg)
                        print(f"Sending: {msg}")
                        self.__retrieve_server_ack()
                        logging.debug(
                            f'action: sent line | result: success | msg: {msg}')
                    except OSError as e:
                        logging.error(
                            f'action: sent line | result: fail | error: {e}')
                    rows = []
            if len(rows) != 0:
                if self._sigterm:
                    return
                try:
                    msg = protocol.encode_registers_batch(rows, AIRPORT_REGISTER)
                    self._send_exact(msg)
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
                if self._sigterm:
                    return
                rows.append(row)
                if len(rows) == BATCH_SIZE:
                    try:
                        msg = protocol.encode_registers_batch(rows, FLIGHT_REGISTER)
                        self._send_exact(msg)
                        self.__retrieve_server_ack()
                        logging.debug(
                            f'action: sent line | result: success | msg: {msg}')
                    except OSError as e:
                        logging.error(
                            f'action: sent line | result: fail | error: {e}')
                    rows = []
            if len(rows) != 0:
                if self._sigterm:
                    return
                try:
                    msg = protocol.encode_registers_batch(rows, FLIGHT_REGISTER)
                    self._send_exact(msg)
                    self._retrieve_server_ack()
                    if self._sigterm:
                        return
                    logging.debug(
                        f'action: sent line | result: success | msg: {msg}')
                except OSError as e:
                    logging.error(
                        f'action: sent line | result: fail | error: {e}')
                rows = []
        self.__send_eof(EOF_FLIGHTS_FILE)
        self.__retrieve_server_ack()
