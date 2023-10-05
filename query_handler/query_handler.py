import json

from util.constants import EOF_FLIGHTS_FILE
from util.queue_middleware import QueueMiddleware


class QueryHandler:

    def __init__(self):
        self.__queue_middleware = QueueMiddleware()

    def run(self, output_queue):
        self.__queue_middleware.listen_on(output_queue, self.__callback)

    def __callback(self, body):
        result = json.loads(body)
        if result.get("op_code") == EOF_FLIGHTS_FILE:
            self.__queue_middleware.finish()
            return
        result.pop('queryNumber', None)
        result.pop('op_code', None)
        print(result)
