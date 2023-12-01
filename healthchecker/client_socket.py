import socket
import time

from client.client import Client


class ClientSocket(Client):
    def __init__(self, address):
        super().__init__(address)

    def run(self):
        pass
