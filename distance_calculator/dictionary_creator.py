import json
import signal
import os

from util.constants import EOF_AIRPORTS_FILE, BEGIN_EOF, NUMBER_CLIENTS
from util.file_manager import log_to_file
from util.initialization import initialize_exchanges, initialize_queues
from util.queue_middleware import QueueMiddleware
from util.recovery_logging import correct_last_line


class DictionaryCreator:
    def __init__(self, input_exchange, name, pipe):
        self.__middleware = QueueMiddleware()
        self.__airports_distances = {key: dict() for key in range(1, NUMBER_CLIENTS + 1)}
        self.__exchange_queue = name + "_airports_queue"
        self.__pipe = pipe
        self.__input_exchange = input_exchange
        self.airport_state_log = "distance_calculator/" + name + "_state_log.txt"
        self.name = name
        self.total_lens = [-1] * NUMBER_CLIENTS

    def run(self):
        signal.signal(signal.SIGTERM, self.__middleware.handle_sigterm)
        initialize_exchanges([self.__input_exchange], self.__middleware)
        initialize_queues([self.__exchange_queue],
                          self.__middleware)
        self.__recover_state()
        self.__middleware.subscribe(self.__input_exchange,
                                    self.__airport_callback,
                                    self.__exchange_queue)

    def __airport_callback(self, body, method):
        register = json.loads(body)
        client_id = register["client_id"]
        if register["op_code"] == EOF_AIRPORTS_FILE:
            required_length = register["message_id"] - 1
            log_to_file(self.airport_state_log, f"{BEGIN_EOF},{register.get('message_id')},"
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
        log_to_file(self._get_logging_file(client_id), str(delimiter.join([airport_code,
                                                                           latitude,
                                                                           longitude])))
        airport_dictionary = self.__airports_distances[client_id]
        airport_dictionary[airport_code] = (latitude, longitude)
        self.send_if_complete(client_id)

    def send_if_complete(self, client_id):
        required_length = self.total_lens[client_id-1]
        if required_length == -1:
            return
        complete_dictionary = (required_length == len(self.__airports_distances[client_id]))
        if complete_dictionary:
            self.__pipe.send((client_id, self.__airports_distances[client_id]))

    def __recover_state(self):
        for client_id in range(1, NUMBER_CLIENTS+1):
            if os.path.exists(self._get_logging_file(client_id)):
                correct_last_line(self._get_logging_file(client_id))
                with open(self._get_logging_file(client_id), 'r') as file:
                    for line in file:
                        if line.endswith("#\n"):
                            continue
                        airport_code, latitude, longitude = tuple(line.split(","))
                        self.__airports_distances[client_id][airport_code] = (latitude, longitude)
        if os.path.exists(self.airport_state_log):
            correct_last_line(self.airport_state_log)
            with open(self.airport_state_log, 'r') as file:
                for line in file:
                    if line.endswith("#\n"):
                        continue
                    _, total_len, client_id = tuple(line.split(","))
                    client_id = int(client_id) - 1
                    self.total_lens[client_id] = total_len
                    for client_id in range(1, NUMBER_CLIENTS+1):
                        self.send_if_complete(client_id)
        return

    def _get_logging_file(self, client_id):
        return f"distance_calculator/{self.name}_airport_log_{client_id}.txt"
