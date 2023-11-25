import ast
import json
import signal

from util.file_manager import save_to_file
from util.initialization import initialize_queues
from util.queue_middleware import QueueMiddleware
from util.utils_query_3 import handle_query_3_register
from util.utils_query_4 import handle_query_4_register, handle_query_4
from util.utils_query_5 import handle_query_5
from util.constants import EOF_FLIGHTS_FILE, EOF_SENT, BEGIN_EOF
from util.recovery_logging import log_to_file

BATCH_SIZE = 10000
CLIENT_NUMBER = 3


class ReducerGroupBy:

    def __init__(self, field_group_by, input_queue,
                 output_queue, query_number, name):
        self.queue_middleware = QueueMiddleware()
        self.field_group_by = field_group_by
        self.output_queue = output_queue
        self.grouped = [dict() for _ in range(CLIENT_NUMBER)]
        self.files = []
        self.input_queue = input_queue
        self.query_number = query_number
        self.operations_map = {4: handle_query_4,
                               5: handle_query_5}
        self.handlers_map = {3: handle_query_3_register,
                             4: handle_query_4_register}
        self.state_log_filename = "reducer_group_by/" + name + "_state_log.txt"
        self.result_log_filename = "reducer_group_by/" + name + "_result_log.txt"
        self.n_clients = CLIENT_NUMBER
        self.__tmp_flights = []
        self.processed_clients = []

    def run(self):
        signal.signal(signal.SIGTERM, self.queue_middleware.handle_sigterm)
        initialize_queues([self.output_queue, self.input_queue],
                          self.queue_middleware)
        self.initialize_result_log()
        #self.recover_state()
        self.queue_middleware.listen_on(self.input_queue, self.__callback)

    def __callback(self, body, method):
        flight = json.loads(body)
        op_code = flight.get("op_code")
        client_id = flight.get("client_id")
        if op_code == EOF_FLIGHTS_FILE:
            self.handle_client_eof(flight, method)
            return
        self.handlers_map[self.query_number](flight, self.grouped[client_id-1], self.result_log_filename)
        self.queue_middleware.manual_ack(method)

    def __read_file(self):
        with open(self.__filename, "r") as file:
            for line in file:
                flight = json.loads(line)
                flight_group_by_field = flight[self.field_group_by]
                flights_list = self.grouped.pop(
                    flight_group_by_field, [])
                flights_list.append(flight)
                self.grouped[flight_group_by_field] = flights_list

    def __read_file_and_send(self):
        for flight_file in self.files:
            all_flights = []
            with open(flight_file, "r") as file:
                for line in file:
                    all_flights.append(json.loads(line))
            msg = self.operations_map.get(self.query_number,
                                          lambda _: None)(all_flights)
            self.queue_middleware.send_message(self.output_queue,
                                               json.dumps(msg))

    def save_flights_to_file(self, flight_list):
        for flight in flight_list:
            filename = flight[self.field_group_by] + ".txt"
            if filename not in self.files:
                self.files.append(filename)
            with open(filename, "a") as file:
                file.write(json.dumps(flight) + '\n')

    def handle_client_eof(self, register, method):
        message_id = register.get('message_id')
        client_id = register.get('client_id')
        log_to_file(self.state_log_filename, f"{BEGIN_EOF},{message_id},{client_id}")
        eof = self.generate_result_message(client_id)
        self.queue_middleware.send_message(self.output_queue, json.dumps(eof))
        self.queue_middleware.manual_ack(method)
        log_to_file(self.state_log_filename, f"{EOF_SENT},{message_id},"f"{client_id}")

    def generate_result_message(self, client_id):
        with open(self.result_log_filename, "r") as file:
            lines = file.readlines()
            eof_message = ast.literal_eval(lines[client_id-1])
        eof_message["client_id"] = client_id
        eof_message["query_number"] = self.query_number
        return eof_message

    def initialize_result_log(self):
        with open(self.result_log_filename, "w") as file:
            for i in range(self.n_clients):
                file.write('\n')
