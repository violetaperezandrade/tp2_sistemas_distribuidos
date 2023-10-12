import json
import signal

from util.constants import EOF_FLIGHTS_FILE, FLIGHT_REGISTER
from util.initialization import initialize_exchanges, initialize_queues
from util.queue_middleware import (QueueMiddleware)


class FilterByAverage:
    def __init__(self, output_queue, input_queue, input_exchange):
        self.__output_queue = output_queue
        self.__input_queue = input_queue
        self.__input_exchange = input_exchange
        self.__middleware = QueueMiddleware()

    def run(self, avg_exchange):
        signal.signal(signal.SIGTERM, self.__middleware.handle_sigterm)
        initialize_exchanges([self.__input_exchange, avg_exchange], self.__middleware)
        initialize_queues([self.__output_queue, self.__input_queue,
                           "cleaned_column_queue"], self.__middleware)
        self.__middleware.subscribe_without_consumption(self.__input_exchange,
                                                        "cleaned_column_queue")
        self.__middleware.subscribe(avg_exchange, self.__callback_avg)
        self.__middleware.listen_on("cleaned_column_queue", self.__callback_filter)

    def __callback_avg(self, body):
        self.__avg = json.loads(body)["avg"]
        self.__middleware.finish(True)

    def __callback_filter(self, body):
        flight = json.loads(body)
        op_code = flight.get("op_code")
        if op_code > FLIGHT_REGISTER:
            return
        if op_code == EOF_FLIGHTS_FILE:
            self.__handle_eof(flight)
            return
        if float(flight["totalFare"]) > self.__avg:
            self.__middleware.send_message(self.__output_queue, body)

    def __handle_eof(self, flight):
        remaining_nodes = flight.get("remaining_nodes")
        if remaining_nodes == 1:
            self.__middleware.send_message(self.__output_queue, json.dumps(flight))
        else:
            flight["remaining_nodes"] -= 1
            self.__middleware.send_message(self.__input_queue, json.dumps(flight))
        self.__middleware.finish()
