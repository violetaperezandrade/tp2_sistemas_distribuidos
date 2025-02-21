import ast
import json
import signal
import os
import re
import time
from file_read_backwards import FileReadBackwards

from util.recovery_logging import correct_last_line, check_files_single_line, go_to_sleep, delete_client_data
from util.initialization import initialize_queues
from util.queue_middleware import QueueMiddleware
from util.utils_query_3 import handle_query_3_register
from util.utils_query_4 import handle_query_4_register, handle_query_4
from util.utils_query_5 import handle_query_5
from util.constants import EOF_FLIGHTS_FILE, NUMBER_CLIENTS
from util.file_manager import log_to_file

BATCH_SIZE = 10000


class ReducerGroupBy:

    def __init__(self, field_group_by, input_queue,
                 output_queue, query_number, name):
        self.queue_middleware = QueueMiddleware()
        self.field_group_by = field_group_by
        self.output_queue = output_queue
        self.grouped = [dict() for _ in range(NUMBER_CLIENTS)]
        self.files = []
        self.input_queue = input_queue
        self.query_number = query_number
        self.operations_map = {4: handle_query_4,
                               5: handle_query_5}
        self.handlers_map = {3: handle_query_3_register,
                             4: handle_query_4_register}
        self.recovery_map = {3: self.recover_state_q3,
                             4: self.recover_state_q4,
                             5: self.recover_state_q5}
        self.callback_map = {3: self.callback_query_3,
                             4: self.callback_query_4,
                             5: self.callback_query_5}
        self.state_log_filename = f"reducer_group_by/{name}_state_log.txt"
        self.result_log_filename = f"reducer_group_by/{name}_result_log.txt"
        self.n_clients = NUMBER_CLIENTS
        self.flights_log_filename = f"reducer_group_by/{name}flights_log.txt"
        self.processed_clients = []
        self.name = name
        self.query_4_results = dict()

    def run(self):
        signal.signal(signal.SIGTERM, self.queue_middleware.handle_sigterm)
        initialize_queues([self.output_queue, self.input_queue],
                          self.queue_middleware)
        self.recover_state()
        os.makedirs(f"reducer_group_by/", exist_ok=True)
        self.queue_middleware.listen_on(self.input_queue, self.__callback)

    def __callback(self, body, method):
        flight = json.loads(body)
        self.callback_map[self.query_number](flight, method)

    def callback_query_3(self, flight, method):
        op_code = flight.get("op_code")
        client_id = flight.get("client_id")
        if int(client_id) in self.processed_clients:
            return
        if op_code == EOF_FLIGHTS_FILE:
            self.spread_eof(client_id, method)
            return

        result_log_filename = self.get_result_log_filename_client(client_id)
        old_filename = (
            f"reducer_group_by/{client_id}/old_results"
            f"_{self.name}_{client_id}.txt")
        tmp_filename = (
            f"reducer_group_by/{client_id}/temp_results"
            f"_{self.name}_{client_id}.txt")
        self.handlers_map[self.query_number](flight, self.grouped[client_id-1],
                                             result_log_filename,
                                             self.name,
                                             old_filename,
                                             tmp_filename)

        self.queue_middleware.manual_ack(method)

    def get_result_log_filename_client(self, client_id):
        return (
            f"reducer_group_by/{client_id}/{self.name}"
            f"_result_log_{client_id}.txt")

    def callback_query_5(self, flight, method):
        op_code = flight.get("op_code")
        client_id = flight.get("client_id")
        if int(client_id) in self.processed_clients:
            self.queue_middleware.manual_ack(method)
            return
        if op_code == EOF_FLIGHTS_FILE:
            self.spread_eof(client_id, method)
            return
        if self.processed_flight(flight):
            self.queue_middleware.manual_ack(method)
            return
        self.save_in_airport_file(flight)
        self.handle_client_message(flight)
        self.queue_middleware.manual_ack(method)

    def generate_q3_result_message(self, client_id, method):
        result_log_filename = self.get_result_log_filename_client(client_id)
        with open(result_log_filename, "r") as file:
            for line in file:
                result = ast.literal_eval(line)
        eof_message = {"result": result,
                       "client_id": client_id,
                       "query_number": self.query_number,
                       "result_id": f"{self.name}_{client_id}"}
        self.queue_middleware.send_message(self.output_queue,
                                           json.dumps(eof_message))
        log_to_file(self.state_log_filename, f"{client_id}")
        delete_client_data(file_path=self.get_result_log_filename_client(client_id))
        self.processed_clients.append(int(client_id))
        if method:
            self.queue_middleware.manual_ack(method)

    def spread_eof(self, client_id, method=None):
        if self.query_number == 3:
            self.generate_q3_result_message(client_id, method)
        elif self.query_number == 4:
            self.generate_q4_result_message(client_id, method)
            self.delete_client_files(client_id)
            self.clean_client_info(client_id)
        elif self.query_number == 5:
            self.generate_q5_result_message(client_id, method)
            self.delete_client_files(client_id)
            self.clean_client_info(client_id)

    def recover_state(self):
        self.recovery_map[self.query_number]()

    def recover_state_q3(self):
        if os.path.exists(self.state_log_filename):
            correct_last_line(self.state_log_filename)
            with open(self.state_log_filename, 'r') as file:
                for line in file:
                    if line.endswith("#\n"):
                        continue
                    self.processed_clients.append(int(line))
                    delete_client_data(file_path=self.get_result_log_filename_client(int(line)))
        self.processed_clients = list(set(self.processed_clients))
        existing_clients = []
        for client_id in range(1, self.n_clients+1):
            dir = f"/reducer_group_by/{client_id}"
            if not os.path.isdir(dir) and client_id not in self.processed_clients:
                os.makedirs(dir)
            else:
                existing_clients.append(client_id)
        for client_id in existing_clients:
            if client_id not in self.processed_clients:
                check_files_single_line(f"/reducer_group_by/{client_id}",
                                        f"{self.name}_result_log_{client_id}.txt")
                if os.path.exists(self.get_result_log_filename_client(client_id)):
                    with open(self.get_result_log_filename_client(client_id), 'r') as file:
                        line = file.read().rstrip('#')
                        self.grouped[client_id-1] = ast.literal_eval(line)

    def add_client_id_to_dir(self, dir, client_id):
        pattern = re.compile(r'(/)')
        return re.sub(pattern, r'/{}\1'.format(client_id), dir, count=1)

    def recover_state_q5(self):
        self.flights_received = dict()
        data = self.recover_process_state_file()
        self.handle_unfinished_eof(data)
        self.recover_processing_clients_data(data)

    def handle_unfinished_eof(self, data):
        for client_id in data.keys():
            if os.path.isdir(f"reducer_group_by/{self.name}/client_{client_id}"):
                airport_log_file = os.listdir(f"reducer_group_by/{self.name}/client_{client_id}")
                if len(airport_log_file) == len(data[client_id]):
                    log_to_file(self.state_log_filename, f"{client_id}")
                    self.delete_client_files(client_id)
                    self.clean_client_info(client_id)
                    continue
                for airport in airport_log_file:
                    airport_code = airport.split(".")[0]
                    if airport_code not in data[client_id]:
                        self.handle_airport_file(client_id, airport)
                log_to_file(self.state_log_filename, f"{client_id}")
                self.processed_clients.append(int(client_id))
                self.delete_client_files(client_id)
                self.clean_client_info(client_id)

    def recover_processing_clients_data(self, data):
        for client_id in range(1, self.n_clients + 1):
            if client_id not in data.keys() and int(client_id) not in self.processed_clients and os.path.isdir(f"reducer_group_by/{self.name}/client_{client_id}"):
                airport_log_file = os.listdir(f"reducer_group_by/{self.name}/client_{client_id}")
                self.recover_processed_client_airports_q5(client_id, airport_log_file)

    def recover_processed_client_airports_q5(self, client_id, airports_logs):
        if client_id not in self.flights_received.keys():
            self.flights_received[client_id] = set()
        for airport in airports_logs:
            filename = f"reducer_group_by/{self.name}/client_{client_id}/{airport}"
            correct_last_line(filename)
            with open(filename, 'r') as f:
                for line in f:
                    if line.endswith("#\n"):
                        continue
                    message_id = int(line.split(",")[0])
                    self.flights_received[client_id].add(message_id)

    def recover_process_state_file(self):
        client_finished = set()
        client_unfinished = dict()
        if os.path.exists(self.state_log_filename):
            correct_last_line(self.state_log_filename)
            with open(self.state_log_filename, 'r') as f:
                lines = f.readlines()
                for line in lines[::-1]:
                    if "#\n" in line:
                        continue
                    line = line.replace("\n", "")
                    data = line.split(",")
                    client_id = int(data[0])
                    if len(data) == 1:
                        client_finished.add(client_id)

                    elif len(data) == 2 and client_id not in client_finished:
                        if client_id not in client_unfinished.keys():
                            client_unfinished[client_id] = set()
                            client_unfinished[client_id].add(data[1])
                        else:
                            client_unfinished[client_id].add(data[1])

            self.processed_clients = list(client_finished)
        return client_unfinished

    def save_in_airport_file(self, flight):
        filename = f"reducer_group_by/{self.name}/client_{flight['client_id']}/{flight[self.field_group_by]}.txt"
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "a+") as file:
            file.write(f"{flight['message_id']},{flight['baseFare']}\n")
            file.flush()

    def generate_q5_result_message(self, client_id, method):
        sent_first_log = False
        for airport in os.listdir(f"reducer_group_by/{self.name}/client_{client_id}"):
            self.handle_airport_file(client_id, airport)
            # send ack after writing fist line in state log   
            if method and not sent_first_log:
                sent_first_log = True
                self.queue_middleware.manual_ack(method)
        log_to_file(self.state_log_filename, f"{client_id}")

    def handle_client_message(self, flight):
        message_id = flight["message_id"]
        client_id = flight["client_id"]
        self.flights_received[client_id].add(message_id)

    def handle_airport_file(self, client_id, airport):
        base_fares = []
        with open(f"reducer_group_by/{self.name}/client_{client_id}/{airport}", 'r') as f:
            for line in f:
                if line.endswith("#\n"):
                    continue
                base_fares.append(float(line.replace("\n", "").split(",")[1]))

        message = handle_query_5(airport.split(".")[0], base_fares)
        message["client_id"] = client_id
        message["query_number"] = self.query_number
        message["result_id"] = f"{self.name}_{client_id}_{airport.split('.')[0]}"
        self.queue_middleware.send_message(self.output_queue,
                                        json.dumps(message))
        log_to_file(self.state_log_filename, f"{client_id},{airport.split('.')[0]}")

    def callback_query_4(self, flight, method):
        op_code = flight.get("op_code")
        client_id = flight.get("client_id")
        if int(client_id) in self.processed_clients:
            self.queue_middleware.manual_ack(method)
            return
        if op_code == EOF_FLIGHTS_FILE:
            self.spread_eof(client_id, method)
            return
        if self.processed_flight(flight):
            self.queue_middleware.manual_ack(method)
            return
        self.handle_flight_avg(flight)
        self.save_in_route_file_q4(flight)
        self.queue_middleware.manual_ack(method)

    def processed_flight(self, flight):
        client_id = flight.get("client_id")
        message_id = flight.get("message_id")
        if client_id not in self.flights_received.keys():
            self.flights_received[client_id] = set()

        return message_id in self.flights_received[client_id]

    def save_in_route_file_q4(self, flight):
        filename = f"reducer_group_by/{self.name}/client_{flight['client_id']}/{flight['route']}.txt"
        client_id = flight.get("client_id")
        message_id = flight["message_id"]
        route = flight.get("route")
        line = f"{message_id},{self.query_4_results[client_id][route]['sum']},{self.query_4_results[client_id][route]['max']},{self.query_4_results[client_id][route]['count']}"
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "a+") as file:
            file.write(f"{line}\n")
            file.flush()

    def handle_flight_avg(self, flight):
        client_id = flight.get("client_id")
        message_id = flight.get("message_id")
        route = flight.get("route")
        self.handle_avg_results(client_id, route, float(flight["totalFare"]))
        self.flights_received[client_id].add(message_id)

    def handle_avg_results(self, client_id, route, totalFare):
        if client_id not in self.query_4_results.keys():
            self.query_4_results[client_id] = dict()
        if route not in self.query_4_results[client_id].keys():
            self.query_4_results[client_id][route] = dict()
            self.query_4_results[client_id][route]["sum"] = 0
            self.query_4_results[client_id][route]["count"] = 0
            self.query_4_results[client_id][route]["max"] = 0
        
        self.query_4_results[client_id][route]["sum"] += totalFare 
        self.query_4_results[client_id][route]["count"] += 1
        self.query_4_results[client_id][route]["max"] = max(totalFare, self.query_4_results[client_id][route]["max"])

    def generate_q4_result_message(self, client_id, method):
        sent_first_log = False
        for filename in os.listdir(f"reducer_group_by/{self.name}/client_{client_id}"):
            self.handle_route_avg(client_id, filename.split(".")[0])
            # send ack after writing fist line in state log
            if method and not sent_first_log:
                sent_first_log = True
                self.queue_middleware.manual_ack(method)
        log_to_file(self.state_log_filename, f"{client_id}")
        self.processed_clients.append(int(client_id))

    def handle_route_file(self, client_id, route):
        sum = 0
        count = 1
        avg = 0
        max_tFare = 0
        filename = f"reducer_group_by/{self.name}/client_{client_id}/{route}"
        with FileReadBackwards(filename, encoding="utf-8") as f:
            for line in f:
                if "#\n" not in line:
                    values = line.split(",")
                    sum = float(values[1])
                    max_tFare = float(values[2])
                    count = int(values[3])
                    if count > 0:
                        avg = "{:.10f}".format(float(sum / count))[:-7]
                    break

        message = dict()
        message["max"] = max_tFare
        message["route"] = route.split(".")[0]
        message["avg"] = avg
        message["client_id"] = client_id
        message["query_number"] = self.query_number
        message["result_id"] = f"{self.name}_{client_id}_{route}"
        self.queue_middleware.send_message(self.output_queue,
                                           json.dumps(message))
        log_to_file(self.state_log_filename, f"{client_id},{route.split('.')[0]}")

    def handle_route_avg(self, client_id, route):
        message = dict()
        sum = self.query_4_results[client_id][route]["sum"]
        count = self.query_4_results[client_id][route]["count"]
        message["max"] = self.query_4_results[client_id][route]["max"]
        message["route"] = route.split(".")[0]
        message["avg"] = "{:.10f}".format(float(sum / count))[:-7]
        message["client_id"] = client_id
        message["query_number"] = self.query_number
        message["result_id"] = f"{self.name}_{client_id}_{route}"
        self.queue_middleware.send_message(self.output_queue,
                                        json.dumps(message))
        log_to_file(self.state_log_filename, f"{client_id},{route.split('.')[0]}")

    def recover_state_q4(self):
        self.flights_received = dict()
        data = self.recover_process_state_file()
        self.handle_unfinished_eof_q4(data)
        self.recover_processing_clients_data_q4(data)

    def handle_unfinished_eof_q4(self, data):
        for client_id in data.keys():
            if os.path.isdir(f"reducer_group_by/{self.name}/client_{client_id}"):
                route_log_file = os.listdir(f"reducer_group_by/{self.name}/client_{client_id}")
                if len(route_log_file) == len(data[client_id]):
                    log_to_file(self.state_log_filename, f"{client_id}")
                    continue
                for route in route_log_file:
                    route_code = route.split(".")[0]
                    if route_code not in data[client_id]:
                        self.handle_route_file(client_id, route)
                log_to_file(self.state_log_filename, f"{client_id}")
                self.processed_clients.append(client_id)

    def recover_processing_clients_data_q4(self, data):
        for client_id in range(1, self.n_clients + 1):
            if client_id not in data.keys() and client_id not in self.processed_clients and os.path.isdir(f"reducer_group_by/{self.name}/client_{client_id}"):
                log_files = os.listdir(f"reducer_group_by/{self.name}/client_{client_id}")
                self.recover_processed_client_avg_q4(client_id,log_files)

    def recover_processed_client_avg_q4(self, client_id, route_logs):
        if client_id not in self.flights_received.keys():
            self.flights_received[client_id] = set()
        for file in route_logs:
            filename = f"reducer_group_by/{self.name}/client_{client_id}/{file}"
            correct_last_line(filename)
            with open(filename, 'r') as f:
                for line in f:
                    if line.endswith("#\n"):
                        continue
                    route = file.split(".")[0]
                    values = line.split(",")
                    if client_id not in self.query_4_results.keys():
                        self.query_4_results[client_id] = dict()
                    
                    if route not in self.query_4_results[client_id].keys():
                        self.query_4_results[client_id][route] = dict()
                        self.query_4_results[client_id][route]["sum"] = 0
                        self.query_4_results[client_id][route]["count"] = 0
                        self.query_4_results[client_id][route]["max"] = 0
                    
                    self.query_4_results[client_id][route]["sum"] = float(values[1])
                    self.query_4_results[client_id][route]["max"] = float(values[2])
                    self.query_4_results[client_id][route]["count"] = int(values[3])
                    self.flights_received[client_id].add(int(values[0]))

    def delete_client_files(self, client_id):
        dirname = f"reducer_group_by/{self.name}/client_{client_id}"
        if os.path.isdir(dirname):
            client_files = os.listdir(dirname)
            for filename in client_files:
                if os.path.exists(f"{dirname}/{filename}"):
                    os.remove(f"{dirname}/{filename}")
            os.rmdir(dirname)
            clients_dirs = os.listdir(f"reducer_group_by/{self.name}")
            if len(clients_dirs) == 0:
                os.rmdir(f"reducer_group_by/{self.name}")

    def clean_client_info(self, client_id):
        if client_id in self.flights_received.keys():
            del self.flights_received[client_id]
        if self.query_number == 4:
            del self.query_4_results[client_id]
