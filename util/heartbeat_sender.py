import time
import struct
from .client_socket import ClientSocket


class HeartbeatSender():
    def __init__(self, node_id, ips, port, frequency):
        self.__id = node_id
        self.__hosts = ips
        self.__port = port + node_id
        self.frequency = frequency
        self.__idx = 0

    def __send_heartbeats(self):
        host = self.__hosts[self.__idx]
        while True:
            try:
                listener_socket = ClientSocket((host, self.__port))
                listener_socket._start_connection()
                print("Connected, sending heartbeats, host:"
                      f" {self.__hosts[self.__idx]}, port:{self.__port}")

                while True:
                    heartbeat = struct.pack('>B', self.__id)
                    listener_socket._send_exact(heartbeat)
                    time.sleep(self.frequency)

            except Exception:
                self.__update_idx()
                host = self.__hosts[self.__idx]
                time.sleep(2)

    def __set_up_port(self):
        host = self.__hosts[self.__idx]
        # print(f"Hosts: {self.__hosts}")
        # print(f"Set up port, actual: {host}, port: {self.__port}")

        try:
            skt = ClientSocket((host, self.__port))
            heartbeat = struct.pack('>B', self.__id)
            skt._send_exact(heartbeat)

            skt._read_exact(1)
            skt._close()
        except Exception:
            self.__update_idx()
            # time.sleep(1)

    def __update_idx(self):
        self.__idx = (self.__idx + 1) % len(self.__hosts)

    def start(self):
        # self.__set_up_port()
        self.__send_heartbeats()
