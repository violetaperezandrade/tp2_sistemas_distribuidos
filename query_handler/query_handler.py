import json

from util.constants import EOF_FLIGHTS_FILE, SIGTERM
from util.queue_middleware import QueueMiddleware


class QueryHandler:

    def __init__(self, query_number):
        self.query_number = query_number
        self.__output_queue = f"output_{query_number}"
        self.__middleware = QueueMiddleware()

    def run(self):
        self.__middleware.listen_on(self.__output_queue, self.__callback)

    def __callback(self, body):
        result = json.loads(body)
        op_code = result.get("op_code")
        if op_code == SIGTERM:
            print("Received sigterm")
            print(result)
            self.__middleware.finish()
            return

        if op_code == EOF_FLIGHTS_FILE:
            self.__middleware.finish()
            return
        result.pop('op_code', None)
        print(f'QUERY {self.query_number}: {result}')
