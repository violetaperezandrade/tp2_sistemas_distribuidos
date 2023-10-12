import logging
import signal

from client import Client
from util import protocol


class ListenerClient(Client):
    def __init__(self, address, query_number):
        super().__init__(address)  # Call the constructor of the abstract class
        self._query_number = query_number

    def run(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        self._start_connection()
        try:
            while not self._sigterm:
                self.__retrieve_result()
        except OSError:
            if self._sigterm:
                # TODO: handle
                logging.info('action: sigterm received')
            else:
                raise
            return
        finally:
            logging.info(
                'action: done with file | result: success')
            self._close_connection()

    def __retrieve_result(self):
        try:
            header = self._read_exact(2)
            payload = self._read_exact(int.from_bytes(header, byteorder='big'))
            result = protocol.decode_query_result(payload)
            if result == b'\x00':
                return
            print(f"QUERY {self._query_number}: {result}")
        except OSError as e:
            logging.error(
                f"action: receive_result | result: fail | error: {e}")

    def _handle_sigterm(self, signum, frame):
        logging.info(
            f'action: sigterm received | signum: {signum}, frame:{frame}')
        self._sigterm = True
        logging.info('action: close_client | result: success')
        return
