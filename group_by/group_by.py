import json
from hashlib import sha256
import signal
from math import floor
import os
from file_read_backwards import FileReadBackwards

from util.file_manager import log_to_file
from util.initialization import initialize_exchanges, initialize_queues
from util.queue_middleware import QueueMiddleware
from util.constants import (EOF_FLIGHTS_FILE, AIRPORT_REGISTER, BEGIN_EOF,
                            EOF_SENT, EOF_CLIENT, FLIGHT_REGISTER)
from util.recovery_logging import go_to_sleep, correct_last_line

REDUCER_ID = 1
MESSAGES_SENT = 2
CLIENT_ID = 3
CLIENTS = 3


class GroupBy():
    def __init__(self, fields_group_by, input_exchange,
                 reducers_amount, queue_group_by, listening_queue,
                 input_queue, name, requires_several_eof):
        self.queue_middleware = QueueMiddleware()
        self.input_exchange = input_exchange
        self.input_queue = input_queue
        self.reducers_amount = reducers_amount
        self.reducers = [
            f"{queue_group_by}_{i}" for i in range(1, reducers_amount + 1)]
        self.listening_queue = listening_queue
        self.group_by_id = (fields_group_by == [""])
        self.handle_group_by_fields(fields_group_by)
        self.requires_several_eof = requires_several_eof
        self.state_log_filename = "group_by/" + name + "_state_log.txt"
        self.flights_log_filename = "group_by/" + name + "_flights_log.txt"
        self.necessary_lines = dict()
        self.reducer_messages_per_client = dict()

    def handle_group_by_fields(self, fields_group_by):
        if len(fields_group_by) > 1:
            self.fields_list = fields_group_by
            self.field_group_by = 'route'
        else:
            self.fields_list = None
            self.field_group_by = fields_group_by[0]

    def run(self):
        signal.signal(signal.SIGTERM, self.queue_middleware.handle_sigterm)

        initialize_exchanges([self.input_exchange], self.queue_middleware)
        initialize_queues([self.listening_queue, self.input_queue] +
                          self.reducers, self.queue_middleware)
        if self.requires_several_eof:
            self.recover_state()
        if self.input_queue == '':  # reading from an exchange
            self.queue_middleware.subscribe(self.input_exchange,
                                            self.__callback,
                                            self.listening_queue)
        elif self.input_exchange == '':  # listening from a queue
            self.queue_middleware.listen_on(self.input_queue, self.__callback)

    def __callback(self, body, method):
        flight = json.loads(body)
        message_id = flight.get("message_id")
        client_id = flight.get('client_id')
        op_code = flight.get("op_code")
        if op_code > FLIGHT_REGISTER:
            self.queue_middleware.manual_ack(method)
            return
        if op_code == EOF_FLIGHTS_FILE:
            if self.requires_several_eof:
                self.handle_several_eofs(flight)
            else:
                self.handle_eof(flight)
            self.queue_middleware.manual_ack(method)
            return
        if self.fields_list is not None:
            flight[self.field_group_by] = self.__create_route(flight,
                                                              self.fields_list)
        if self.group_by_id:
            output_queue = self.__get_output_queue_with_message_id(flight)
        else:
            output_queue = self.__get_output_queue(flight, self.field_group_by)
        self.queue_middleware.send_message(self.reducers[output_queue],
                                           json.dumps(flight))
        if self.requires_several_eof:
            self.handle_reducer_message_per_client(client_id, output_queue, flight, message_id)
        self.queue_middleware.manual_ack(method)

    def __create_route(self, flight, fields_list):
        return "-".join(flight[str(field)] for field in fields_list)

    def handle_eof(self, flight):
        message_id = flight.get("message_id")
        client_id = flight.get("client_id")
        log_to_file(self.state_log_filename, f"{BEGIN_EOF},{message_id},{client_id}")
        messages_sent = floor((flight["message_id"] - 1) / self.reducers_amount)
        module = (flight["message_id"] - 1) % self.reducers_amount
        for reducer in self.reducers:
            flight["messages_sent"] = messages_sent
            if (int(reducer[-1])) <= module:
                flight["messages_sent"] = messages_sent + 1
            self.queue_middleware.send_message(reducer, json.dumps(flight))
        log_to_file(self.state_log_filename, f"{EOF_SENT},{message_id},{client_id}")

    def __get_output_queue(self, flight, field_group_by):
        field = flight.get(field_group_by)
        hashed_field = int(sha256(field.encode()).hexdigest(), 16)
        return hashed_field % self.reducers_amount

    def __get_output_queue_with_message_id(self, flight):
        message_id = flight.get("message_id")
        module = message_id % self.reducers_amount
        if module != 0:
            return module - 1
        else:
            return 2

    def handle_several_eofs(self, flight):
        messages_sent = flight["messages_sent"]
        client_id = flight["client_id"]
        reducer_id = flight["filter_id"]
        log_to_file(self.state_log_filename, f"{EOF_CLIENT},{reducer_id},{messages_sent},{client_id}")
        self.verify_all_eofs_received(client_id, flight)

    def verify_all_eofs_received(self, client_id, flight=None):
        eofs = set()
        with open(self.state_log_filename, "r") as file:
            for line in file:
                if not line.startswith(str(EOF_SENT)) and line.endswith("\n"):
                    line = line.strip('\n')
                    if line.endswith("#"):
                        continue
                    line = tuple(line.split(','))
                    if int(line[CLIENT_ID]) == int(client_id):
                        eofs.add((line[REDUCER_ID], line[MESSAGES_SENT]))
        if len(eofs) == self.reducers_amount:
            corrected_eof = 0
            for i in eofs:
                corrected_eof += int(i[1])
            self.necessary_lines[client_id] = corrected_eof
            if flight:
                self.send_eof_to_reducers(client_id, flight)

    def send_eof_to_reducers(self, client_id, flight):
        if client_id in self.necessary_lines.keys():
            total = 0
            for value in self.reducer_messages_per_client[client_id]:
                total += value
            if total == self.necessary_lines[client_id]:
                flight["messages_sent"] = 0
                for i, reducer in enumerate(self.reducers):
                    flight["messages_sent"] += self.reducer_messages_per_client[client_id][i]
                    self.queue_middleware.send_message(reducer, json.dumps(flight))
                log_to_file(self.state_log_filename, f"{EOF_SENT},{client_id}")

    def handle_reducer_message_per_client(self, client_id, output_queue, flight, message_id):
        self.reducer_messages_per_client[client_id][output_queue] += 1
        log_reducers_amounts = ",".join(map(str,
                                             self.reducer_messages_per_client[client_id]))
        log_to_file(self.flights_log_filename, f"{message_id},{client_id}"
                    f",{log_reducers_amounts}")
        self.send_eof_to_reducers(client_id, flight)

    def recover_state(self):
        clients_recovered = []
        if os.path.exists(self.flights_log_filename):
            correct_last_line(self.flights_log_filename)
            with FileReadBackwards(self.flights_log_filename, encoding="utf-8") as frb:
                while True:
                    line = frb.readline()
                    if not line:
                        break
                    if line == '\n':
                        continue
                    if line.endswith("#\n"):
                        continue
                    line_list_ = line.split(",")
                    line_list = [int(x) for x in line_list_]
                    message_id = line_list.pop(0)
                    client_id = line_list.pop(0)
                    if client_id in clients_recovered:
                        continue
                    else:
                        self.reducer_messages_per_client[client_id] = line_list
                        clients_recovered.append(client_id)
        if len(clients_recovered) != CLIENTS:
            for i in range(1, CLIENTS+1):
                if i not in clients_recovered:
                    self.reducer_messages_per_client[i] = [0] * self.reducers_amount
        if os.path.exists(self.state_log_filename):
            correct_last_line(self.state_log_filename)
            for client in range(1, CLIENTS+1):
                self.verify_all_eofs_received(client)
        return
