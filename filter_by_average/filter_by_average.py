import ast
import json
import signal

from util.constants import EOF_FLIGHTS_FILE, FLIGHT_REGISTER, BATCH_SIZE
from util.initialization import initialize_exchanges, initialize_queues
from util.queue_middleware import (QueueMiddleware)
from util.file_manager import save_to_file


class FilterByAverage:
    def __init__(self, output_queue, input_queue, input_exchange, node_id):
        self.__output_queue = output_queue
        self.__input_queue = input_queue
        self.__input_exchange = input_exchange
        self.id = node_id
        self.__file_name = "stored_flights.txt"
        self.__tmp_flights = []
        self.__middleware = QueueMiddleware()
        self.sigterm_received = False

    def run(self, avg_exchange):
        receiver_queue = "avg_receiver_" + self.id
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        initialize_exchanges([self.__input_exchange, avg_exchange], self.__middleware)
        initialize_queues([self.__output_queue, self.__input_queue, receiver_queue], self.__middleware)

        self.__middleware.subscribe_without_consumption(avg_exchange, receiver_queue)
        self.__middleware.subscribe(self.__input_exchange, self.__callback_filter, self.__input_queue)
        if self.sigterm_received:
            return
        self.__middleware.listen_on(receiver_queue, self.__callback_avg)
        if self.sigterm_received:
            return
        self.__send_flights_over_average()

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
        self.__tmp_flights.append(flight)
        if len(self.__tmp_flights) >= BATCH_SIZE:
            save_to_file(self.__tmp_flights, self.__file_name)
            self.__tmp_flights = []

    def __handle_eof(self, flight):
        self.__tmp_flights.append(flight)
        save_to_file(self.__tmp_flights, self.__file_name)
        flight["remaining_nodes"] -= 1
        if flight["remaining_nodes"] > 0:
            self.__middleware.send_message(self.__input_queue, json.dumps(flight))
        self.__middleware.finish(True)

    def __send_flights_over_average(self):
        with open(self.__file_name, "r") as file:
            for line in file:
                flight = ast.literal_eval(line)
                if flight["op_code"] == EOF_FLIGHTS_FILE or float(flight["totalFare"]) > self.__avg:
                    self.__middleware.send_message(self.__output_queue, json.dumps(flight))

    def _handle_sigterm(self, signum, frame):
        self.sigterm_received = True
        self.__middleware.handle_sigterm(signum, frame)
