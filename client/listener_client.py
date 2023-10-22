import logging

from client import Client
from util import protocol
from util.constants import EOF_RESULTS, SIGTERM_RESULTS


class ListenerClient(Client):
    def __init__(self, address, query_number):
        super().__init__(address)  # Call the constructor of the abstract class
        self._query_number = query_number
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
            print(f"HEADER: {header}")
            payload = self._read_exact(int.from_bytes(header, byteorder='big'))
            if len(payload) == 1:
                if payload == b'\x07':
                    print(f"Received sigterm: {payload}, I am client: {self._query_number}")
                    self._sigterm = True
                    return
                if payload == b'\x00':
                    print(f"Received EOF: {payload}, I am client: {self._query_number}")
                    self._eof = True
                    return
            result = protocol.decode_query_result(payload)
            return
            # print(f"QUERY {self._query_number}: {result}")
        except OSError as e:
            logging.info(
                f"action: receive_result | result: fail | error: {e}")
