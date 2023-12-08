from time import sleep

from util.client_socket import ClientSocket
from constants import HEARTBEAT, COORDINATOR

def leader_validation(id, addr, leader_id):
    while True:
        try:
            socket = ClientSocket(addr)
            socket._start_connection()
        except OSError:
            sleep(1)
            continue
        first_message_sent = False
        while True:
            try:
                heartbeat = bytes([HEARTBEAT, leader_id])
                if not first_message_sent:
                    heartbeat = bytes([COORDINATOR, leader_id])
                socket._send_exact(heartbeat)
                first_message_sent = True
                sleep(1)
            except (ConnectionResetError, BrokenPipeError, TimeoutError):
                break
