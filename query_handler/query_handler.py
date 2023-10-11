import json

from util.constants import EOF_FLIGHTS_FILE
from util.initialization import initialize_queues
from util.queue_middleware import QueueMiddleware


class QueryHandler:

    def __init__(self, query_number, eof_max):
        self.query_number = query_number
        self.__output_queue = f"output_{query_number}"
        self.__middleware = QueueMiddleware()
        self.__eofs_received = 0
        self.__eof_max = eof_max

    def run(self):
        initialize_queues([self.__output_queue], self.__middleware)
        self.__middleware.listen_on(self.__output_queue, self.__callback)

    def __callback(self, body):
        result = json.loads(body)
        if result.get("op_code") == EOF_FLIGHTS_FILE:
            if self.__eofs_received >= self.__eof_max:
                self.__middleware.finish()
            else:
                self.__eofs_received += 1
            return
        result.pop('op_code', None)
        print(f'QUERY {self.query_number}: {result}')
