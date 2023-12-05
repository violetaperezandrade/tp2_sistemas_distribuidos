import json
import logging
from multiprocessing import Process
import socket
from client_handler import ClientHandler
from util.constants import NUMBER_CLIENTS, CLEANUP, ALL_CLIENTS
from util.queue_middleware import QueueMiddleware


def launch_new_handler(client_socket):
    new_client_handler = ClientHandler(client_socket)
    new_client_handler.run()


class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self.connections = 0
        self.__queue_middleware = QueueMiddleware()

    def run(self):
        cleanup_message = {"op_code": CLEANUP, "client_id": ALL_CLIENTS}
        self.__queue_middleware.create_queue("full_flight_registers")
        self.__queue_middleware.send_message("full_flight_registers",
                                             json.dumps(cleanup_message))
        processes = []
        while self.connections < NUMBER_CLIENTS:
            client_socket = self.__accept_new_connection()
            listener_process = Process(target=launch_new_handler,
                                       args=(client_socket,))
            processes.append(listener_process)
            listener_process.start()
            self.connections += 1
        for process in processes:
            process.join()

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
