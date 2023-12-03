import logging
import json

from util import protocol
from util.constants import (EOF_FLIGHTS_FILE, FLIGHT_REGISTER,
                            AIRPORT_REGISTER, EOF_AIRPORTS_FILE,
                            SIGTERM, SERVER_ACK, INVALID)
from util.queue_middleware import QueueMiddleware


class ClientHandler:
    def __init__(self, client_socket):
        # Initialize server socket
        self.__client_socket = client_socket
        self._running = True
        self._reading_file = True
        self.__queue_middleware = QueueMiddleware()
        self._operations_map = {
            EOF_FLIGHTS_FILE: self.__handle_eof,
            FLIGHT_REGISTER: self.__read_line,
            AIRPORT_REGISTER: self.__read_line,
            EOF_AIRPORTS_FILE: self.__handle_eof,
            SIGTERM: self.__handle_sigterm
        }
        self.client_id = INVALID
        self._register_number = 1

    def run(self):
        self.__queue_middleware.create_queue("full_flight_registers")
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

    def __read_line(self, registers):
        for register in registers:
            register["message_id"] = self._register_number
            if self.client_id == INVALID:
                self.client_id = register["client_id"]
            self._register_number += 1
            self.__queue_middleware.send_message("full_flight_registers",
                                                 json.dumps(register))

    def __handle_eof(self, payload):
        opcode = protocol.get_opcode(payload)
        msg = protocol.encode_eof(opcode, self._register_number, self.client_id)
        self.__queue_middleware.send_message("full_flight_registers",
                                             msg)
        if opcode == EOF_AIRPORTS_FILE:
            self._register_number = 1

    def __send_ack(self):
        msg = protocol.encode_server_ack(SERVER_ACK)
        self.__send_exact(msg)

    def __handle_sigterm(self, registers):
        self._running = False
        logging.info('action: close_server | result: success')
        return
