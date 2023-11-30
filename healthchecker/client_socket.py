import socket
import time

from client.client import Client


class ClientSocket(Client):
    def __init__(self, address, timeout=0):
        super().__init__(address)
        self._client_socket.settimeout(timeout)

    def run(self):
        pass
