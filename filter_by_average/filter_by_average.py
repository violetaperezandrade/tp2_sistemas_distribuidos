
import json
import os
import signal

from util.constants import EOF_FLIGHTS_FILE, NUMBER_CLIENTS, EOF_SENT, BEGIN_EOF, READ_SIZE
from util.initialization import initialize_queues
from util.queue_middleware import (QueueMiddleware)
from util.file_manager import log_to_file
from util.recovery_logging import correct_last_line, create_eof_flights_message_filters, get_state_log_file, \
    get_flights_log_file, delete_client_data


class FilterByAverage:
    def __init__(self, output_queue, input_queue, node_id, name, total_reducers, pipe, process):
        self.__output_queue = output_queue
        self.__input_queue = f"{input_queue}_{node_id}"
        self.__id = node_id
        self.__middleware = QueueMiddleware()
        self.__name = name
        self.__pipe = pipe
        self.__total_reducers = total_reducers
        self.__averages = {i: 0.0 for i in range(1, NUMBER_CLIENTS + 1)}
        self.__lines_processed = {i: 0 for i in range(1, NUMBER_CLIENTS + 1)}
        self.__lines_accepted = {i: 0 for i in range(1, NUMBER_CLIENTS + 1)}
        self.__missing_flights = {i: set() for i in range(1, NUMBER_CLIENTS + 1)}
        self.__eof_received = {i: False for i in range(1, NUMBER_CLIENTS + 1)}
        self.__accepted_flights = {i: 0 for i in range(1, NUMBER_CLIENTS + 1)}
        self.main_path = f"filter_by_average/{self.__name}"
        self.processed_clients=set()
        self.process = process

    def run(self):
        signal.signal(signal.SIGTERM, self.handle_sigterm)
        initialize_queues([self.__output_queue, self.__input_queue], self.__middleware)
        self.recover_sent_state()
        os.makedirs(self.main_path, exist_ok=True)
        self.__middleware.listen_on(self.__input_queue, self.__callback_filter)

    def __callback_filter(self, body, method):
        flight = json.loads(body)
        op_code = int(flight.get("op_code"))
        client_id = int(flight.get("client_id"))
        message_id = int(flight.get("message_id"))
        if ((message_id not in self.__missing_flights[client_id] and self.__eof_received[client_id])
                or client_id in self.processed_clients):
            self.__middleware.manual_ack(method)
            return
        if op_code == EOF_FLIGHTS_FILE:
            self.get_avg_for_client(client_id)
            self.__eof_received[client_id] = True
            log_to_file(get_state_log_file(self.main_path), f"{BEGIN_EOF},{message_id},{client_id}")
            self.get_missing_flights_and_send(message_id, client_id)
            self.send_and_log_eof(self.__accepted_flights[client_id], client_id, message_id)
            self.__middleware.manual_ack(method)
            return
        log_to_file(get_flights_log_file(self.main_path, client_id), json.dumps(flight))
        if self.__eof_received[client_id]:
            if len(self.__missing_flights[client_id]) > 0:
                if float(flight["totalFare"]) > self.__averages[client_id]:
                    self.__accepted_flights[client_id] += 1
                    self.__middleware.send_message(self.__output_queue,
                                                   json.dumps(flight))
                self.__missing_flights[client_id].remove(message_id)
            self.__lines_processed[client_id] += 1
            log_to_file(self.get_filtering_log_file(client_id), f"{self.__lines_processed[client_id]},"
                                                                f"{self.__accepted_flights[client_id]}")
            self.send_and_log_eof(self.__accepted_flights[client_id], client_id, message_id)
        self.__middleware.manual_ack(method)

    def get_avg_for_client(self, client_id):
        if self.__averages[client_id] != 0.0:
            return
        while True:
            msg = self.__pipe.recv()
            alt_client_id = msg["client_id"]
            alt_avg = msg["avg"]
            self.__averages[alt_client_id] = alt_avg
            if alt_client_id != client_id:
                continue
            break

    def get_missing_flights_and_send(self, eof_message_id, client_id, only_reconstruct=False):
        for i in range(self.__id, eof_message_id, self.__total_reducers):
            self.__missing_flights[client_id].add(i)
        file = get_flights_log_file(self.main_path, client_id)
        correct_last_line(file)
        loop_number = 1
        lines_currently_processed = self.__lines_processed[client_id]
        with open(file, "r") as flights_file:
            for line in flights_file:
                if line.endswith("#\n"):
                    continue
                flight = json.loads(line)
                message_id = flight["message_id"]
                if only_reconstruct:
                    if loop_number <= lines_currently_processed:
                        self.__missing_flights[client_id].discard(message_id)
                        if loop_number == lines_currently_processed:
                            return
                        else:
                            loop_number += 1
                            continue
                self.__lines_processed[client_id] += 1
                if message_id not in self.__missing_flights[client_id]:
                    continue
                self.__missing_flights[client_id].remove(message_id)
                if float(flight["totalFare"]) > self.__averages[client_id]:
                    self.__middleware.send_message(self.__output_queue,
                                                   json.dumps(flight))
                    self.__accepted_flights[client_id] += 1
                if self.__lines_processed[client_id] % READ_SIZE == 0:
                    log_to_file(self.get_filtering_log_file(client_id),
                                f"{self.__lines_processed[client_id]},"
                                f"{self.__accepted_flights[client_id]}")
        modulus = self.__lines_processed[client_id] % READ_SIZE
        if modulus != 0:
            log_to_file(self.get_filtering_log_file(client_id), f"{self.__lines_processed[client_id]},"
                                                                f"{self.__accepted_flights[client_id]}")

    def send_and_log_eof(self, accepted_flights, client_id, message_id):
        if len(self.__missing_flights[client_id]) == 0:
            eof = create_eof_flights_message_filters(accepted_flights, self.__id, client_id)
            eof["message_id"] = int(message_id)
            self.__middleware.send_message(self.__output_queue, json.dumps(eof))
            log_to_file(get_state_log_file(self.main_path), f"{EOF_SENT},{eof.get('client_id')}")
            self.processed_clients.add(client_id)
            delete_client_data(file_path=get_flights_log_file(self.main_path, client_id))
            delete_client_data(file_path=self.get_avg_file(client_id))
            delete_client_data(file_path=self.get_filtering_log_file(client_id))

    def recover_sent_state(self):
        for i in range(1, NUMBER_CLIENTS + 1):
            filepath = self.get_filtering_log_file(i)
            if os.path.exists(filepath):
                correct_last_line(filepath)
                with open(filepath, "r") as filtering_log:
                    for line in filtering_log:
                        if line.endswith("#\n"):
                            continue
                        try:
                            processed_lines, accepted_lines, index = tuple(line.split(","))
                        except ValueError as e:
                            continue
                        processed_lines = int(processed_lines)
                        accepted_lines = int(accepted_lines)
                        self.__lines_processed[i] = processed_lines
                        self.__accepted_flights[i] = accepted_lines
        filepath = get_state_log_file(self.main_path)
        recovered_clients = []
        if os.path.exists(filepath):
            correct_last_line(filepath)
            with open(filepath, "r") as state_file:
                lines = state_file.readlines()
                for line in lines:
                    if line.endswith("#\n"):
                        continue
                    try:
                        opcode, message_id, client_id = tuple(line.split(","))
                    except ValueError as e:
                        continue
                    opcode = int(opcode)
                    message_id = int(message_id)
                    client_id = int(client_id)
                    if client_id in recovered_clients:
                        continue
                    if opcode == BEGIN_EOF:
                        if f"{EOF_SENT},{client_id}\n" in lines:
                            self.processed_clients.add(client_id)
                            delete_client_data(file_path=get_flights_log_file(self.main_path, client_id))
                            delete_client_data(file_path=self.get_avg_file(client_id))
                            delete_client_data(file_path=self.get_filtering_log_file(client_id))
                            continue
                        self.get_avg_for_client(client_id)
                        self.__eof_received[client_id] = True
                        self.get_missing_flights_and_send(message_id, client_id, True)
                        self.send_and_log_eof(self.__accepted_flights[client_id], client_id, message_id)
                        recovered_clients.append(client_id)

    def get_filtering_log_file(self, client_id):
        file = f"{self.main_path}/client_{client_id}_filtering_log.txt"
        return file

    def get_avg_file(self, client_id):
        file = f"{self.main_path}/client_{client_id}_avg_log.txt"
        return file

    def handle_sigterm(self, signum, frame):
        os.kill(self.process.pid, signal.SIGTERM)
        self.__middleware.handle_sigterm(signum, frame)