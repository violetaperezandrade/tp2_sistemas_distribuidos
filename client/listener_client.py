import logging

from client import Client
from util import protocol
# from util.constants import EOF_RESULTS, SIGTERM_RESULTS


class ListenerClient(Client):
    def __init__(self, address):
        super().__init__(address)  # Call the constructor of the abstract class
        self._eof = False

    def run(self):
        self._start_connection()
        try:
            while not self._sigterm and not self._eof:
                self.__retrieve_result()
        except OSError:
            if self._sigterm:
                logging.info('action: sigterm received')
                logging.info('action: close_client | result: success')
            else:
                raise
            return
        finally:
            logging.info(
                'action: done with results | result: success')
            self._close_connection()

    def __retrieve_result(self):
        try:
            header = self._read_exact(2)
            payload = self._read_exact(int.from_bytes(header, byteorder='big'))
            if len(payload) == 1:
                if payload == b'\x07':
                    self._sigterm = True
                    return
                if payload == b'\x00':
                    self._eof = True
                    return
            result = protocol.decode_query_result(payload)
            query_number = result.pop('query_number')
            print(f"QUERY {query_number}:"
                  f"{result}")
        except OSError as e:
            logging.info(
                f"action: receive_result | result: fail | error: {e}")
