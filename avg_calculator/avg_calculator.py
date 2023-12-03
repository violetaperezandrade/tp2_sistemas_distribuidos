import os

from util.constants import EOF_FLIGHTS_FILE, NUMBER_CLIENTS, BEGIN_EOF, EOF_SENT
from util.file_manager import log_to_file
from util.initialization import initialize_queues, initialize_exchanges
from util.queue_middleware import (QueueMiddleware)
import json
import signal

from util.recovery_logging import get_missing_flights_for_avg_calculation, correct_last_line, \
    get_updated_sum_and_count, delete_client_data


class AvgCalculator:
    def __init__(self, column_name, output_exchange,
                 input_queue, id, name, reducers_amount):
        self.__column_name = column_name
        self.__output_exchange = output_exchange
        self.reducers_amount = reducers_amount
        self.__id = id
        self.__input_queue = f"{input_queue}_{id}"
        self.__name = name
        self.__missing_flights = [set() for _ in range(NUMBER_CLIENTS)]
        self.__middleware = QueueMiddleware()
        self.sum = [0.0] * NUMBER_CLIENTS
        self.count = [0] * NUMBER_CLIENTS
        self.eof_status = [False] * NUMBER_CLIENTS
        self.__processed_clients = []

    def run(self):
        signal.signal(signal.SIGTERM, self.__middleware.handle_sigterm)
        initialize_queues([self.__input_queue], self.__middleware)
        initialize_exchanges([self.__output_exchange], self.__middleware)
        self.recover_state()
        self.__middleware.listen_on(self.__input_queue, self.__callback)

    def __callback(self, body, method):
        flight = json.loads(body)
        op_code = flight.get("op_code")
        client_id = int(flight.get("client_id"))
        message_id = int(flight.get("message_id"))
        index = int(client_id) - 1
        if ((message_id not in self.__missing_flights[index] and self.eof_status[index])
                or client_id in self.__processed_clients):
            self.__middleware.manual_ack(method)
            return
        if op_code == EOF_FLIGHTS_FILE:
            log_to_file(self.get_state_log_file(client_id), f"{BEGIN_EOF},{message_id}")
            self.eof_status[index] = True
            self.sum[index], self.count[index] = get_missing_flights_for_avg_calculation(
                self.get_flights_log_file(client_id),
                self.__missing_flights[index],
                self.__id,
                self.reducers_amount,
                message_id)
            self.send_and_log_partial_avg(client_id, message_id)
            self.__middleware.manual_ack(method)
            return
        self.sum[index] += float(flight[self.__column_name])
        self.count[index] += 1
        log_to_file(self.get_flights_log_file(client_id),
                    f"{message_id},{self.sum[index]},{self.count[index]}")
        if self.eof_status[index]:
            if len(self.__missing_flights[index]) > 0:
                self.__missing_flights[index].remove(message_id)
            self.send_and_log_partial_avg(client_id, message_id)
        self.__middleware.manual_ack(method)

    def create_partial_avg_message(self, client_id, message_id):
        partial_avg = dict()
        partial_avg["message_id"] = message_id
        partial_avg["op_code"] = EOF_FLIGHTS_FILE
        partial_avg["count"] = self.count[client_id - 1]
        partial_avg["sum"] = self.sum[client_id - 1]
        partial_avg["client_id"] = client_id
        partial_avg["filter_id"] = self.__id
        return partial_avg

    def recover_state(self):
        for client in range(1, NUMBER_CLIENTS + 1):
            state_log_file = self.get_state_log_file(client)
            flights_log_file = self.get_flights_log_file(client)
            index = client - 1
            if os.path.exists(state_log_file):
                correct_last_line(state_log_file)
                with open(state_log_file, 'r') as file:
                    lines = file.readlines()
                    for line in lines:
                        if line.endswith("#\n"):
                            continue
                        try:
                            opcode, message_id = tuple(line.split(","))
                        except ValueError as e:
                            continue
                        if int(opcode) == BEGIN_EOF:
                            if client in self.__processed_clients:
                                continue
                            if f"{EOF_SENT}\n" in lines:
                                delete_client_data(client, file_path=flights_log_file)
                                break
                            self.eof_status[index] = True
                            self.sum[index], self.count[index] = get_missing_flights_for_avg_calculation(
                                flights_log_file,
                                self.__missing_flights[index],
                                self.__id,
                                self.reducers_amount,
                                int(message_id))
                            self.send_and_log_partial_avg(client, message_id)
            else:
                if os.path.exists(flights_log_file):
                    self.sum[index], self.count[index] = get_updated_sum_and_count(flights_log_file)

    def send_and_log_partial_avg(self, client_id, message_id):
        if len(self.__missing_flights[client_id - 1]) == 0:
            eof = self.create_partial_avg_message(client_id, message_id)
            self.__middleware.publish(self.__output_exchange, json.dumps(eof))
            log_to_file(self.get_state_log_file(client_id), f"{EOF_SENT}")
            self.__processed_clients.append(client_id)
            flights_log_file = self.get_flights_log_file(client_id)
            delete_client_data(client_id, file_path=flights_log_file)

    def get_flights_log_file(self, client_id):
        self.create_if_necessary(client_id)
        file = f"avg_calculator/{self.__name}/client_{client_id}/flights_log.txt"
        return file

    def get_state_log_file(self, client_id):
        self.create_if_necessary(client_id)
        file = f"avg_calculator/{self.__name}/client_{client_id}/state_log.txt"
        return file

    def create_if_necessary(self, client_id, path=None):
        path = f"avg_calculator/{self.__name}/client_{client_id}"
        if not os.path.exists(path):
            os.makedirs(path)
