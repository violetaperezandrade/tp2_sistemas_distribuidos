import socket
from client.client import Client


class ClientSocket(Client):
    def __init__(self, address):
        super().__init__(address)

    def run(self):
        pass

    def _close(self):
        try:
            self._client_socket.shutdown(socket.SHUT_RDWR)
        except Exception:
            pass
        self._client_socket.close()
