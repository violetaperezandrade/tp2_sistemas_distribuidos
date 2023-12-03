import logging
import os

from client import Client
from util import protocol
from util.constants import EOF_B, SIGTERM_B, NUMBER_CLIENTS


class ListenerClient(Client):
    def __init__(self, address):
        super().__init__(address)
        self._eof = False

    def run(self):
        self._start_connection()
        file_list = []
        for client_number in range(1, NUMBER_CLIENTS + 1):
            os.makedirs(os.path.dirname(f"results/client_{client_number}/"), exist_ok=True)
            for i in range(1, 6):
                file_list.append(f"results/client_{client_number}/query_{i}.txt")
        try:
            files = [open(i, 'w') for i in file_list]
            while not self._sigterm and not self._eof:
                self.__retrieve_result(files)
            for file in files:
                file.close()
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
            query_number = result.get('query_number')
            # result_id = result.pop("result_id", None)
            # message_id = result.pop("message_id", None)
            client_id = result.get("client_id", None)
            # print(f"QUERY {query_number}:"
            #       f"{result}")
            index = 5 * (client_id-1) + query_number
            files[index-1].write(str(result) + '\n')
            files[index-1].flush()
        except OSError as e:
            logging.info(
                f"action: receive_result | result: fail | error: {e}")
