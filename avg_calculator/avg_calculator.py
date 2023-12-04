import os

from util.constants import EOF_FLIGHTS_FILE, NUMBER_CLIENTS, BEGIN_EOF, EOF_SENT
from util.file_manager import log_to_file
from util.initialization import initialize_queues, initialize_exchanges
from util.queue_middleware import (QueueMiddleware)
import json
import signal

from util.recovery_logging import get_missing_flights_for_avg_calculation, correct_last_line, \
    delete_client_data, get_state_log_file, get_flights_log_file


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
        self.main_path = f"avg_calculator/{self.__name}"
        self.__processed_clients = []

    def run(self):
        signal.signal(signal.SIGTERM, self.__middleware.handle_sigterm)
        initialize_queues([self.__input_queue], self.__middleware)
        initialize_exchanges([self.__output_exchange], self.__middleware)
        self.recover_state()
        os.makedirs(self.main_path, exist_ok=True)
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
            log_to_file(get_state_log_file(self.main_path), f"{BEGIN_EOF},{message_id},{client_id}")
            self.eof_status[index] = True
            self.sum[index], self.count[index] = get_missing_flights_for_avg_calculation(
                get_flights_log_file(self.main_path, client_id),
                self.__missing_flights[index],
                self.__id,
                self.reducers_amount,
                message_id)
            self.send_and_log_partial_avg(client_id, message_id)
            self.__middleware.manual_ack(method)
            return
        self.sum[index] += float(flight[self.__column_name])
        self.count[index] += 1
        log_to_file(get_flights_log_file(self.main_path, client_id),
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
        state_log_file = get_state_log_file(self.main_path)
        if os.path.exists(state_log_file):
            correct_last_line(state_log_file)
            with open(state_log_file, 'r') as file:
                lines = file.readlines()
                for line in lines:
                    if line.endswith("#\n"):
                        continue
                    try:
                        opcode, message_id, client_id = tuple(line.split(","))
                    except ValueError as e:
                        continue
                    client_id = int(client_id)
                    index = client_id - 1
                    if int(opcode) == BEGIN_EOF:
                        if client_id in self.__processed_clients:
                            continue
                        flights_log_file = get_flights_log_file(self.main_path, client_id)
                        if f"{EOF_SENT},{client_id}\n" in lines:
                            self.__processed_clients.append(client_id)
                            delete_client_data(client_id, file_path=flights_log_file)
                            continue
                        self.eof_status[index] = True
                        self.sum[index], self.count[index] = get_missing_flights_for_avg_calculation(
                            flights_log_file,
                            self.__missing_flights[index],
                            self.__id,
                            self.reducers_amount,
                            int(message_id))
                        self.send_and_log_partial_avg(client_id, message_id)

    def send_and_log_partial_avg(self, client_id, message_id):
        if len(self.__missing_flights[client_id - 1]) == 0:
            eof = self.create_partial_avg_message(client_id, message_id)
            self.__middleware.publish(self.__output_exchange, json.dumps(eof))
            log_to_file(get_state_log_file(self.main_path), f"{EOF_SENT},{client_id}")
            flights_log_file = get_flights_log_file(self.main_path,
                                                    client_id)
            self.__processed_clients.append(client_id)
            delete_client_data(file_path=flights_log_file)
