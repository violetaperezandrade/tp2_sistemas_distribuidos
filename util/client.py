import socket
import logging
import time

MSG_LEN = 1024


class Client:
    def __init__(self, address):
        # Initialize client socket
        self._client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_address = address

    def start_client_loop(self):
        self.__start_connection_with_server()
        for i in range(0, 5):
            time.sleep(3)    
            message = "Message {}".format(i)
            self.__run(message)
        
        self.__close_connection()

    def __run(self, message):
        try:
            self.__send_msg(message)
            logging.info(f'action: receive_message | result: success | msg: {message}')
            msg = self.__recv_msg()
        except OSError as e:
            logging.error("action: receive_message | result: fail | error: {e}")


    def __start_connection_with_server(self):
        """
        Start connection with Server

        Function connect TCP client with server
        """

        # Connection arrived
        logging.info(f"action: starts_connection | host: {self._server_address[0]} | port: {self._server_address[1]} | result: in_progress")
        self._client_socket.connect(self._server_address)
        logging.info(f'action: starts_connection | host: {self._server_address[0]} | port: {self._server_address[1]} | result: success ')

    def __close_connection(self):
        """
        Close connection

        Function close TCP client connection with server
        """

        # Connection arrived
        logging.debug('action: close_connection | result: in_progress')
        self._client_socket.close()
        logging.info(f'action: close_connection | result: success ')

    """receive message from server socket"""
    def __recv_msg(self):
        result = b''
        remaining = MSG_LEN
        while remaining > 0:
            data = self._client_socket.recv(remaining)
            result += data
            remaining -= len(data)
        
        return result.decode('utf-8').replace("X", "")

    """creates message for server socket"""
    def __generate_message(self, message):
        return message.ljust(MSG_LEN, 'X')

    """send response message to server"""
    def __send_msg(self, message):
        response = self.__generate_message(message).encode('utf-8')
        remaining = MSG_LEN
        while remaining > 0:
            pos = MSG_LEN - remaining
            nBytesSent = self._client_socket.send(response[pos:MSG_LEN])
            logging.debug(f'action: sending_message | result: success | message: {message} | bytes_sent: {nBytesSent}')
            remaining -= nBytesSent
