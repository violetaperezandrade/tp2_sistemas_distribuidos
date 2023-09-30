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
        """
        Dummy Server loop

        Server that accept a new connections and establishes a
        communication with a client. After client with communucation
        finishes, servers starts to accept new connections again
        """

        # TODO: Modify this program to handle signal to graceful shutdown
        # the server
        client_sock = self.__accept_new_connection()
        for i in range(0, 5):
            self.__handle_client_connection(client_sock)
        
        client_sock.close()

    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        try:
            msg = self.__recv_msg(client_sock)
            addr = client_sock.getpeername()
            logging.info(f'action: receive_message | result: success | ip: {addr[0]} | msg: {msg}')
            self.__send_msg(client_sock, msg)
        except OSError as e:
            logging.error("action: receive_message | result: fail | error: {e}")

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """

        # Connection arrived
        logging.info('action: accept_connections | result: in_progress')
        c, addr = self._server_socket.accept()
        logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
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
    def __generate_res(self, message):
        return message.ljust(MSG_LEN, 'X')

    """send response message to client"""
    def __send_msg(self, sock, message):
        response = self.__generate_res(message).encode('utf-8')
        remaining = MSG_LEN
        while remaining > 0:
            pos = MSG_LEN - remaining
            nBytesSent = sock.send(response[pos:MSG_LEN])
            logging.info(f'action: sending_response | result: success | message: {message} | bytes_sent: {nBytesSent}')
            remaining -= nBytesSent
