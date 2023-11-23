import socket
import logging
import json

from util import protocol
from util.constants import (EOF_FLIGHTS_FILE, FLIGHT_REGISTER,
                            AIRPORT_REGISTER, EOF_AIRPORTS_FILE,
                            SIGTERM, SERVER_ACK)
from util.queue_middleware import QueueMiddleware


class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self.__queue_middleware = QueueMiddleware()
        self.__client_socket = None
        self._running = True
        self._reading_file = True
        self._operations_map = {
            EOF_FLIGHTS_FILE: self.__handle_eof,
            FLIGHT_REGISTER: self.__read_line,
            AIRPORT_REGISTER: self.__read_line,
            EOF_AIRPORTS_FILE: self.__handle_eof,
            SIGTERM: self.__handle_sigterm
        }
        self._register_number = 1

    def run(self):
        self.__queue_middleware.create_queue("full_flight_registers")
        self.__client_socket = self.__accept_new_connection()
        try:
            while self._running and self._reading_file:
                self.__handle_client_connection()
        except OSError:
            if not self._running:
                logging.info('action: sigterm received')
            else:
                raise
            return
        finally:
            logging.info(
                'action: done with file | result: success')
            self.__client_socket.close()

    def __handle_client_connection(self):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        try:
            header = self.__read_exact(3)
            payload = self.__read_exact(int.from_bytes(header,
                                                       byteorder='big'))
            registers, op_code = protocol.get_opcode_batch(payload)
            data = self._operations_map.get(op_code, lambda _: 0)(registers)
            self.__send_ack()
            # TODO: error handling
            if data == 0:
                logging.error(
                    "action: receive_message | result: fail | "
                    f"error: received unknown operation code: {op_code}")
        except OSError as e:
            logging.error(
                f"action: receive_message | result: fail | error: {e}")
            pass

    def __read_exact(self, bytes_to_read):
        bytes_read = self.__client_socket.recv(bytes_to_read)
        while len(bytes_read) < bytes_to_read:
            new_bytes_read = self.__client_socket.recv(
                bytes_to_read - len(bytes_read))
            bytes_read += new_bytes_read
        return bytes_read

    def __send_exact(self, answer):
        bytes_sent = 0
        while bytes_sent < len(answer):
            chunk_size = self.__client_socket.send(answer[bytes_sent:])
            bytes_sent += chunk_size

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """

        # Connection arrived
        logging.info('action: accept_connections | result: in_progress')
        c, addr = self._server_socket.accept()
        logging.info(
            f'action: accept_connections | result: success | ip: {addr[0]}')
        return c

    def __read_line(self, registers):
        for register in registers:
            register["message_id"] = self._register_number
            self._register_number += 1
            self.__queue_middleware.send_message("full_flight_registers",
                                                 json.dumps(register))

    def __handle_eof(self, payload):
        opcode = protocol.get_opcode(payload)
        msg = protocol.encode_eof(opcode, self._register_number)
        self.__queue_middleware.send_message("full_flight_registers", msg)
        if opcode == EOF_FLIGHTS_FILE:
            self._reading_file = False

    def __send_ack(self):
        msg = protocol.encode_server_ack(SERVER_ACK)
        self.__send_exact(msg)

    def __handle_sigterm(self, registers):
        self._running = False
        logging.info('action: close_server | result: success')
        return
