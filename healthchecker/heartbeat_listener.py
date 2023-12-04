import socket
import logging
import subprocess
import time

from server_utils import read_exact
from util.nodes_utils import get_node_from_idx

NODES_R = ["group_by_id", "initial_column_cleaner",
           "filter_by_three_stopovers", "reducer_group_by_route",
           "group_by_id"]
NODES_U = ["group_by_route", "query_handler"]


class HeartbeatListener():
    def __init__(self, listen_port, node_id, timeout):
        # Initialize server socket
        self._listener_socket = socket.socket(socket.AF_INET,
                                              socket.SOCK_STREAM)
        self._listener_socket.bind(('', listen_port))
        self._listener_socket.listen(1)
        self._listener_socket.settimeout(timeout)
        print(f"LISTEN PORT: {listen_port}")
        self._node_id = node_id
        self._timeout = timeout

    def start(self):
        node_socket = self.__accept_new_connection()

        while True:
            try:
                node_socket.settimeout(self._timeout)
                read_exact(node_socket, 1)
            except Exception as e:
                print(repr(e))
                print(f"Got exception: {type(e).__name__}")
                self.restart_node(self._node_id)
                node_socket = self.__accept_new_connection()
                while not node_socket:
                    self.restart_node(self._node_id)
                    node_socket = self.__accept_new_connection()

    def restart_node(self, node_id):
        node_name = get_node_from_idx(node_id)
        print(f"Will restart node {node_name}")
        result = subprocess.run(['docker', 'start', node_name],
                                check=False,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        logging.info('Command executed. Result={}. Output={}. Error={}'.format(result.returncode,
                                                                               result.stdout,
                                                                               result.stderr))

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """
        try:
            # Connection arrived
            print("Accepting new connection")
            c, addr = self._listener_socket.accept()
            print(f"Accepting new connection ip: {addr[0]}")
            logging.info(
                f'action: accept_connection | ip: {addr[0]}')
            return c
        except socket.timeout:
            return None

    def _read_exact(self, bytes_to_read):
        bytes_read = self._client_socket.recv(bytes_to_read)
        while len(bytes_read) < bytes_to_read:
            new_bytes_read = self._client_socket.recv(
                bytes_to_read - len(bytes_read))
            if new_bytes_read == 0:
                raise BrokenPipeError
            bytes_read += new_bytes_read
        return bytes_read

    def _close(self):
        try:
            self._client_socket.shutdown(socket.SHUT_RDWR)
        except Exception:
            pass
        self._client_socket.close()
