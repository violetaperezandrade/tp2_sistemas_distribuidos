from client.client import Client


class ClientSocket(Client):
    def __init__(self, address):
        super().__init__(address)
        self._client_socket.settimeout(10)

    def run(self):
        return
