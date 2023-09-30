import socket
import logging

from util.queue_middleware import QueueMiddleware

MSG_LEN = 1024


class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self.rabbitmq_mw = QueueMiddleware()

    def run(self):
        client_sock = self.__accept_new_connection()
        self.__handle_client_connection(client_sock)
        client_sock.close()

    def __handle_client_connection(self, client_sock):
        try:
            while True:
                self.read_line(client_sock)
            addr = client_sock.getpeername()
            logging.info(
                f'action: receive_message | result: success | ip: {addr[0]} | msg: {msg}')
        except OSError as e:
            logging.error(
                "action: receive_message | result: fail | error: {e}")
        except Exception:
            logging.info(
                f'action: done with file | result: success')

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

    def read_line(self, sock):
        byte_array = bytearray()
        opcode = self.__recv_msg(sock, 1)
        byte_array += opcode
        opcode = int.from_bytes(opcode, byteorder="big")
        if opcode == 0:
            # raise custom exception
            raise Exception
        logging.info(
            f'action: opcode_received | result: success | opcode: {opcode}')
        msg_size = self.__recv_msg(sock, 2)
        byte_array += msg_size
        msg_size = int.from_bytes(msg_size, byteorder="big")
        logging.info(
            f'action: size_received | result: success | size: {msg_size}')
        read = 0
        while True:
            column_size = self.__recv_msg(sock, 2)
            byte_array += column_size
            column_size = int.from_bytes(column_size, byteorder="big")
            column_value = self.__recv_msg(sock, column_size)
            logging.info(
                f'action: value received | result: success | column: {column_value}')
            read += column_size + 2
            byte_array += column_value
            if read >= msg_size:
                self.rabbitmq_mw.send_message_to(
                    "full_flight_register", byte_array)
                break

    def __recv_msg(self, sock, length):
        result = b''
        remaining = length
        while remaining > 0:
            data = sock.recv(remaining)
            result += data
            remaining -= len(data)
        return result
