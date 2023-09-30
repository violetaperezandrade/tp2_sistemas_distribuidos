import socket
import logging

MSG_LEN = 1024


class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)

    def run(self):
        client_sock = self.__accept_new_connection()
        for i in range(0, 5):
            self.__handle_client_connection(client_sock)

        client_sock.close()

    def __handle_client_connection(self, client_sock):
        try:
            msg = self.__recv_msg(client_sock)
            addr = client_sock.getpeername()
            logging.info(
                f'action: receive_message | result: success | ip: {addr[0]} | msg: {msg}')
        except OSError as e:
            logging.error(
                "action: receive_message | result: fail | error: {e}")

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

    """receive message from client socket"""

    def __recv_msg(self, sock):
        result = b''
        remaining = MSG_LEN
        while remaining > 0:
            data = sock.recv(remaining)
            result += data
            remaining -= len(data)
        return result.decode('utf-8').replace("X", "")

    """creates message for client socket"""
