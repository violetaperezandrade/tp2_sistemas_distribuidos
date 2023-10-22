import json
import signal
import socket

from util.constants import EOF_FLIGHTS_FILE
from util.initialization import initialize_queues
from util.queue_middleware import QueueMiddleware
from util import protocol


class QueryHandler:

    def __init__(self, query_number, eof_max, listen_backlog):
        self.query_number = query_number
        self.__input_queue = f"output_{query_number}"
        self.__middleware = QueueMiddleware()
        self.__eofs_received = 0
        self.__eof_max = eof_max

    def run(self):
        signal.signal(signal.SIGTERM, self.__middleware.handle_sigterm)
        initialize_queues([self.__input_queue, "results"], self.__middleware)
        self.__middleware.listen_on(self.__input_queue, self.__callback)

    def __callback(self, body):
        result = json.loads(body)
        op_code = result.get("op_code")
        if op_code == EOF_FLIGHTS_FILE:
            self.__eofs_received += 1
            if self.__eofs_received == self.__eof_max:
                self.__middleware.send_message("results", json.dumps(result))
                self.__middleware.finish()
            return
        result["query_number"] = self.query_number
        self.__middleware.send_message("results", json.dumps(result))

