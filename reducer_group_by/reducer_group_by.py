import ast
import json
import signal
import os
import time
from file_read_backwards import FileReadBackwards

from util.recovery_logging import correct_last_line, check_files
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
                             4: handle_query_4_register,}
        self.state_log_filename = "reducer_group_by/" + name + "_state_log.txt"
        self.result_log_filename = "reducer_group_by/" + name + "_result_log.txt"
        self.n_clients = NUMBER_CLIENTS
        self.flights_log_filename = "reducer_group_by/" + name + "_flights_log.txt"
        self.processed_clients = []
        self.name = name
        self.query_4_results = dict()
        self.n = 0

    def run(self):
        signal.signal(signal.SIGTERM, self.queue_middleware.handle_sigterm)
        initialize_queues([self.output_queue, self.input_queue],
                          self.queue_middleware)
        self.recover_state()
        self.initialize_result_log()
        self.queue_middleware.listen_on(self.input_queue, self.__callback)

    def __callback(self, body, method):
        flight = json.loads(body)
        if self.query_number == 3:
            self.callback_query_3(flight, method)
        
        elif self.query_number == 4:
            self.callback_query_4(flight, method)
        
        elif self.query_number == 5:
            self.callback_query_5(flight, method)


    def callback_query_3(self, flight, method):
        op_code = flight.get("op_code")
        client_id = flight.get("client_id")
        if int(client_id) in self.processed_clients:
            return
        if op_code == EOF_FLIGHTS_FILE:
            self.spread_eof(client_id, method)
            return
        
        self.handlers_map[self.query_number](flight, self.grouped[client_id-1],
                                             self.result_log_filename, self.name)
 
        self.queue_middleware.manual_ack(method)
 

    def callback_query_5(self, flight, method):
        op_code = flight.get("op_code")
        client_id = flight.get("client_id")
        if int(client_id) in self.processed_clients:
            self.queue_middleware.manual_ack(method)
            return
        if self.processed_flight(flight):
            print(f"procesado {flight['message_id']}")
            self.queue_middleware.manual_ack(method)
            return
        if op_code == EOF_FLIGHTS_FILE:
            self.spread_eof(client_id, method)
            return
        self.save_in_airport_file(flight)
        self.handle_client_message(flight)
        self.queue_middleware.manual_ack(method)


    def generate_q3_result_message(self, client_id, method):
        with open(self.result_log_filename, "r") as file:
            lines = file.readlines()
            result = ast.literal_eval(lines[int(client_id)-1])
        eof_message = {'result': result, "client_id": client_id, "query_number": self.query_number,
                       "result_id": f"{self.name}_{client_id}"}
        self.queue_middleware.send_message(self.output_queue,
                                           json.dumps(eof_message))
        log_to_file(self.state_log_filename, f"{client_id}")
        self.processed_clients.append(int(client_id))
        if method:
            self.queue_middleware.manual_ack(method)

    def initialize_result_log(self):
        if self.query_number == 3:
            if not os.path.exists(self.result_log_filename):
                with open(self.result_log_filename, "w") as file:
                    for i in range(self.n_clients):
                        file.write('\n')
                        file.flush()

    def spread_eof(self, client_id, method=None):
        if self.query_number == 3:
            self.generate_q3_result_message(client_id, method)
        elif self.query_number == 4:
            self.generate_q4_result_message(client_id, method)
        elif self.query_number == 5:
            self.generate_q5_result_message(client_id, method)


    def recover_state(self):
        if self.query_number == 3:
            self.recover_state_q3()
        
        elif self.query_number == 4:
            self.recover_state_q4()

        elif self.query_number == 5:
            self.recover_state_q5()


    def recover_state_q3(self):
        check_files("/reducer_group_by", self.name + "_result_log.txt")
        if os.path.exists(self.state_log_filename):
            correct_last_line(self.state_log_filename)
            with open(self.state_log_filename, 'r') as file:
                for line in file:
                    if line.endswith("#\n"):
                        continue
                    self.processed_clients.append(int(line))
        self.processed_clients = list(set(self.processed_clients))

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
                    continue
                for airport in airport_log_file:
                    airport_code = airport.split(".")[0]
                    if airport_code not in data[client_id]:
                        self.handle_airport_file(client_id, airport)
                log_to_file(self.state_log_filename, f"{client_id}")
                self.processed_clients.append(int(client_id))

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
                    self.flights_received[client_id].add(line.split(",")[0])
 
    def recover_process_state_file(self):
        client_finished = set()
        client_unfinished = dict()
        if os.path.exists(self.state_log_filename):
            correct_last_line(self.state_log_filename)
            with open( self.state_log_filename, 'r') as f:
                lines = f.readlines()
                for line in lines[::-1]:
                    if "#\n" in line:
                        continue
                    line = line.replace("\n", "")
                    data = line.split(",")
                    if len(data) == 1:
                        client_finished.add(data[0])
                    
                    elif len(data) == 2 and data[0] not in client_finished:
                        if data[0] not in client_unfinished.keys():
                            client_unfinished[data[0]] = set()
                            client_unfinished[data[0]].add(data[1])
                        else:
                            client_unfinished[data[0]].add(data[1])

            self.processed_clients = list(client_finished)
        return client_unfinished
    
    def save_in_airport_file(self, flight):
        filename = f"reducer_group_by/{self.name}/client_{flight['client_id']}/{flight[self.field_group_by]}.txt"
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "a+") as file:
            file.write(f"{flight['message_id']},{flight['baseFare']}\n")

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
        with open( f"reducer_group_by/{self.name}/client_{client_id}/{airport}", 'r') as f:
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
        if self.processed_flight(flight):
            print(f"procesado {flight['message_id']}")
            self.queue_middleware.manual_ack(method)
            return
        if op_code == EOF_FLIGHTS_FILE:
            self.spread_eof(client_id, method)
            return
        self.handle_flight_avg(flight)
        self.save_in_route_file_q4(flight)
        if self.n == 150:
            print(flight)
            print("go to sleep")
            time.sleep(60)
        self.queue_middleware.manual_ack(method)
        self.n = self.n + 1
    
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
        print(2)
        print(self.query_4_results)
        if client_id not in self.query_4_results.keys():
            self.query_4_results[client_id] = dict()
        print(3)
        print(self.query_4_results)
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
        for route in os.listdir(f"reducer_group_by/{self.name}/client_{client_id}"):
            self.handle_route_file(client_id, route)
            # send ack after writing fist line in state log
            if method and not sent_first_log:
                sent_first_log = True
                self.queue_middleware.manual_ack(method)
        log_to_file(self.state_log_filename, f"{client_id}")

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
                        avg = sum / count
                    break

        message = dict()
        message["avg"] = avg
        message["max"] = max_tFare
        message["client_id"] = client_id
        message["route"] = route.split(".")[0]
        message["query_number"] = self.query_number
        message["result_id"] = f"{self.name}_{client_id}_{route}"
        self.queue_middleware.send_message(self.output_queue,
                                        json.dumps(message))
        log_to_file(self.state_log_filename, f"{client_id},{route.split('.')[0]}")


    def recover_state_q4(self):
        self.flights_received = dict()
        data = self.recover_process_state_file()
        #print(data)
        self.handle_unfinished_eof_q4(data)
        self.recover_processing_clients_data_q4(data)

    def handle_unfinished_eof_q4(self, data):
        for client_id in data.keys():
            if os.path.isdir(f"reducer_group_by/{self.name}/client_{client_id}"):
                route_log_file = os.listdir(f"reducer_group_by/{self.name}/client_{client_id}")
                if len(route_log_file) == len(data[client_id]):
                    log_to_file(self.state_log_filename, f"{client_id}")
                    continue
                #print(route_log_file)
                for route in route_log_file:
                    route_code = route.split(".")[0]
                    if route_code not in data[client_id]:
                        self.handle_route_file(client_id, route)
                log_to_file(self.state_log_filename, f"{client_id}")
                self.processed_clients.append(int(client_id))

    def recover_processing_clients_data_q4(self, data):
        for client_id in range(1, self.n_clients + 1):
            if client_id not in data.keys() and int(client_id) not in self.processed_clients and os.path.isdir(f"reducer_group_by/{self.name}/client_{client_id}"):
                #print("here 1")
                log_files = os.listdir(f"reducer_group_by/{self.name}/client_{client_id}")
                self.recover_processed_client_avg_q4(client_id,log_files)
        #print(self.flights_received)

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
                    self.query_4_results[client_id][route]["count"] += 1
                    self.flights_received[client_id].add(values[0])
        print(1)
        print(self.query_4_results)