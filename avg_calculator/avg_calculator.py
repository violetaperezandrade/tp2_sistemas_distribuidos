from util.constants import EOF_FLIGHTS_FILE, FLIGHT_REGISTER, AVG_READY
from util.initialization import initialize_exchanges, initialize_queues
from util.queue_middleware import (QueueMiddleware)
import json
import signal


class AvgCalculator:
    def __init__(self, column_name, output_exchange, input_exchange,
                 input_queue):
        self.__column_name = column_name
        self.__output_exchange = output_exchange
        self.__input_exchange = input_exchange
        self.__input_queue = input_queue
        self.__result = dict()
        self.__result["sum"] = 0.0
        self.__result["count"] = 0
        self.__middleware = QueueMiddleware()

    def run(self):
        signal.signal(signal.SIGTERM, self.__middleware.handle_sigterm)
        initialize_exchanges([self.__output_exchange, self.__input_exchange],
                             self.__middleware)
        initialize_queues([self.__input_queue], self.__middleware)
        self.__middleware.subscribe(self.__input_exchange,
                                    self.__callback,
                                    self.__input_queue)

    def __callback(self, body):
        flight = json.loads(body)
        op_code = flight.get("op_code")
        if op_code == AVG_READY:
            self.__handle_avg_res(flight)
            return
        if op_code > FLIGHT_REGISTER:
            return
        if op_code == EOF_FLIGHTS_FILE:
            self.__handle_eof(flight)
            return
        self.__result["sum"] += float(flight[self.__column_name])
        self.__result["count"] += 1

    def __handle_eof(self, flight):
        if flight["remaining_nodes"] == 1:
            flight["sum"] = 0
            flight["count"] = 0
            self.__handle_avg_res(flight)
        flight["sum"] = self.__result["sum"]
        flight["count"] = self.__result["count"]
        flight["op_code"] = AVG_READY
        flight["remaining_nodes"] -= 1
        self.__middleware.send_message(self.__input_queue, json.dumps(flight))
        self.__middleware.finish()
        return

    def __handle_avg_res(self, flight):
        remaining_nodes = flight.get("remaining_nodes")
        flight["sum"] += self.__result["sum"]
        flight["count"] += self.__result["count"]

        if remaining_nodes == 1:
            final_result = flight["sum"] / flight["count"]
            message = self.__generate_mesage(final_result)
            self.__middleware.publish(self.__output_exchange,
                                      json.dumps(message))
            self.__middleware.finish()
            return

        flight["remaining_nodes"] -= 1
        self.__middleware.send_message(self.__input_queue, json.dumps(flight))
        self.__middleware.finish()
        return

    def __generate_mesage(self, avg):
        message = dict()
        message["op_code"] = AVG_READY
        message["avg"] = avg
        return message
