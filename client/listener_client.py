import logging
import os

from client import Client
from util import protocol
from util.constants import EOF_B, SIGTERM_B


class ListenerClient(Client):
    def __init__(self, address):
        super().__init__(address)
        self._eof = False

    def run(self, hostname, client_id):
        self._start_connection()
        self._send_exact(client_id.to_bytes(1, byteorder='big'))
        os.makedirs(os.path.dirname(f"results/{hostname}/"), exist_ok=True)
        try:
            with (open(f"results/{hostname}/query1.txt", mode='w') as file1,
                  open(f"results/{hostname}/query2.txt", mode='w') as file2,
                  open(f"results/{hostname}/query3.txt", mode='w') as file3,
                  open(f"results/{hostname}/query4.txt", mode='w') as file4,
                  open(f"results/{hostname}/query5.txt", mode='w') as file5):
                while not self._sigterm and not self._eof:
                    self.__retrieve_result([file1, file2,
                                            file3, file4,
                                            file5], client_id)
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

    def __retrieve_result(self, files, client_id):
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
            # print(f"QUERY {query_number}: {result}")
            # only for the diff
            if query_number == 1:
                result['totalFare'] = '{:.2f}'.format(
                    float(result['totalFare']))
            files[int(query_number) - 1].write(str(result) + '\n')
            files[int(query_number) - 1].flush()
        except OSError as e:
            logging.info(
                f"action: receive_result | result: fail | error: {e}")
