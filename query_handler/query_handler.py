import json
import signal

from util.constants import EOF_FLIGHTS_FILE
from util.initialization import initialize_queues
from util.queue_middleware import QueueMiddleware


class QueryHandler:

    def __init__(self, query_number):
        self.query_number = query_number
        self.__input_queue = f"output_{query_number}"
        self.__middleware = QueueMiddleware()

    def run(self):
        signal.signal(signal.SIGTERM, self.__middleware.handle_sigterm)
        initialize_queues([self.__input_queue, "results"], self.__middleware)
        self.__middleware.listen_on(self.__input_queue, self.__callback)

    def __callback(self, body, method):
        result = json.loads(body)
        op_code = result.get("op_code")
        if op_code == EOF_FLIGHTS_FILE:
            self.__middleware.manual_ack(method)
            return
        result["query_number"] = self.query_number
        self.__middleware.send_message("results", json.dumps(result))
        self.__middleware.manual_ack(method)
