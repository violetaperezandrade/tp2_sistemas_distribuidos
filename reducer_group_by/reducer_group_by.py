import ast
import json
import signal

from util.file_manager import save_to_file
from util.initialization import initialize_queues
from util.queue_middleware import QueueMiddleware
from util.utils_query_3 import *
from util.utils_query_4 import *
from util.utils_query_5 import handle_query_5
from util.constants import *


class ReducerGroupBy():

    def __init__(self, field_group_by, input_queue, output_queue, query_number):
        self.queue_middleware = QueueMiddleware()
        self.field_group_by = field_group_by
        self.output_queue = output_queue
        self.grouped = {}
        self.input_queue = input_queue
        self.query_number = query_number
        self.operations_map = {3: handle_query_3,
                               4: handle_query_4,
                               5: handle_query_5}
        self.__filename = f"{input_queue}.txt"
        self.__tmp_flights = []

    def run(self):
        signal.signal(signal.SIGTERM, self.queue_middleware.handle_sigterm)
        initialize_queues([self.output_queue, self.input_queue],
                          self.queue_middleware)
        self.queue_middleware.listen_on(self.input_queue, self.__callback)


    def __callback(self, body):
        flight = json.loads(body)
        op_code = flight.get("op_code")
        if len(self.__tmp_flights) >= BATCH_SIZE:
            save_to_file(self.__tmp_flights, self.__filename)
            self.__tmp_flights = []
        if op_code == EOF_FLIGHTS_FILE:
            save_to_file(self.__tmp_flights, self.__filename)
            self.__read_file()
            self.__handle_eof()
            self.queue_middleware.send_message(self.output_queue, body)
            self.queue_middleware.finish()
            return
        self.__tmp_flights.append(flight)


    def __handle_eof(self):
        for route, flights in self.grouped.items():
            msg = self.operations_map.get(self.query_number,
                                          lambda _: None)(flights)
            if msg is None:
                # Error handling
                pass
            if type(msg) is list:
                for message in msg:
                    if message != {}:
                        self.queue_middleware.send_message(self.output_queue,
                                                           json.dumps(message))
            else:
                self.queue_middleware.send_message(self.output_queue,
                                                   json.dumps(msg))

    def __read_file(self):
        with open(self.__filename, "r") as file:
            for line in file:
                flight = ast.literal_eval(line)
                flight_group_by_field = flight[self.field_group_by]
                self.grouped[flight_group_by_field] = self.grouped.get(
                    flight_group_by_field, [])
                self.grouped[flight_group_by_field].append(flight)