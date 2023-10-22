import logging

from client import Client
from util import protocol
from util.constants import EOF_B, SIGTERM_B


class ListenerClient(Client):
    def __init__(self, address):
        super().__init__(address)
        self._eof = False

    def run(self):
        self._start_connection()
        try:
            with (open("results/query_1.txt", mode='a') as file1,
                  open("results/query_2.txt", mode='a') as file2,
                  open("results/query_3.txt", mode='a') as file3,
                  open("results/query_4.txt", mode='a') as file4,
                  open("results/query_5.txt", mode='a') as file5):
                while not self._sigterm and not self._eof:
                    self.__retrieve_result([file1, file2, file3, file4, file5])
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

    def __retrieve_result(self, files):
        try:
            header = self._read_exact(2)
            payload = self._read_exact(int.from_bytes(header, byteorder='big'))
            if len(payload) == 1:
                if payload == SIGTERM_B:
                    self._sigterm = True
                    return
                if payload == EOF_B:
                    self._eof = True
                    return
            result = protocol.decode_query_result(payload)
            query_number = result.pop('query_number')
            print(f"QUERY {query_number}:"
                  f"{result}")
            files[int(query_number) - 1].write(str(result) + '\n')
        except OSError as e:
            logging.info(
                f"action: receive_result | result: fail | error: {e}")
