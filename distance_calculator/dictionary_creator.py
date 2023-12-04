import json
import signal
import os

from util.constants import EOF_AIRPORTS_FILE, BEGIN_EOF, NUMBER_CLIENTS
from util.file_manager import log_to_file
from util.initialization import initialize_exchanges, initialize_queues
from util.queue_middleware import QueueMiddleware
from util.recovery_logging import correct_last_line, get_flights_log_file, get_state_log_file, delete_client_data


class DictionaryCreator:
    def __init__(self, input_exchange, name, pipe):
        self.__middleware = QueueMiddleware()
        self.__airports_distances = {key: dict() for key in range(1, NUMBER_CLIENTS + 1)}
        self.__exchange_queue = name + "_airports_queue"
        self.__pipe = pipe
        self.__input_exchange = input_exchange
        self.name = name
        self.total_lens = [-1] * NUMBER_CLIENTS
        self.main_path = f"distance_calculator/{self.name}"
        self.clients_processed = []

    def run(self):
        signal.signal(signal.SIGTERM, self.__middleware.handle_sigterm)
        initialize_exchanges([self.__input_exchange], self.__middleware)
        initialize_queues([self.__exchange_queue],
                          self.__middleware)
        self.__recover_state()
        os.makedirs(self.main_path, exist_ok=True)
        self.__middleware.subscribe(self.__input_exchange,
                                    self.__airport_callback,
                                    self.__exchange_queue)


    def __airport_callback(self, body, method):
        register = json.loads(body)
        client_id = register["client_id"]
        if client_id in self.clients_processed:
            self.__middleware.manual_ack(method)
            return
        if register["op_code"] == EOF_AIRPORTS_FILE:
            required_length = register["message_id"] - 1
            log_to_file(get_state_log_file(self.main_path), f"{BEGIN_EOF},{register.get('message_id')},"
                                                            f"{register.get('client_id')}")
            self.total_lens[int(client_id) - 1] = required_length
            self.send_if_complete(client_id)
            self.__middleware.manual_ack(method)
            return
        self.__store_value(register)
        self.__middleware.manual_ack(method)

    def __store_value(self, register):
        airport_code = register["Airport Code"]
        latitude = register["Latitude"]
        longitude = register["Longitude"]
        client_id = register["client_id"]
        delimiter = ","
        log_to_file(get_flights_log_file(self.main_path, client_id), str(delimiter.join([airport_code,
                                                                                         latitude,
                                                                                         longitude])))
        airport_dictionary = self.__airports_distances[client_id]
        airport_dictionary[airport_code] = (latitude, longitude)
        self.send_if_complete(client_id)

    def send_if_complete(self, client_id):
        required_length = self.total_lens[client_id - 1]
        if required_length == -1:
            return
        complete_dictionary = (required_length == len(self.__airports_distances[client_id]))
        if complete_dictionary:
            self.__pipe.send((client_id, self.__airports_distances[client_id]))
            self.clients_processed.append(client_id)

    def __recover_state(self):
        for client_id in range(1, NUMBER_CLIENTS + 1):
            flight_log = get_flights_log_file(self.main_path, client_id)
            if os.path.exists(flight_log):
                correct_last_line(flight_log)
                with open(flight_log, 'r') as file:
                    for line in file:
                        if line.endswith("#\n"):
                            continue
                        airport_code, latitude, longitude = tuple(line.split(","))
                        self.__airports_distances[client_id][airport_code] = (latitude, longitude)
        state_log = get_state_log_file(self.main_path)
        if os.path.exists(state_log):
            correct_last_line(state_log)
            with open(state_log, 'r') as file:
                for line in file:
                    if line.endswith("#\n"):
                        continue
                    _, total_len, client_id = tuple(line.split(","))
                    client_id = int(client_id)
                    index = client_id - 1
                    self.total_lens[index] = int(total_len) - 1
                    for client_id in range(1, NUMBER_CLIENTS + 1):
                        self.send_if_complete(client_id)
        return
