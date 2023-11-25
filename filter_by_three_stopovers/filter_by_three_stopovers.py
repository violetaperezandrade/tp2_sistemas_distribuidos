import json
import signal
import os

from util.constants import EOF_FLIGHTS_FILE, FLIGHT_REGISTER, EOF_SENT, FILTERED, ACCEPTED, BEGIN_EOF
from util.file_manager import log_to_file
from util.initialization import initialize_exchanges, initialize_queues
from util.queue_middleware import QueueMiddleware
from util.recovery_logging import get_missing_flights, go_to_sleep

NUMBER_CLIENTS = 3


class FilterByThreeStopovers:
    def __init__(self, columns_to_filter, max_stopovers,
                 output_queue, output_exchange, name,
                 reducers_amount):
        self.__id = int(name[-1])
        self.__max_stopovers = max_stopovers
        self.__columns_to_filter = columns_to_filter
        self.__output_queue = output_queue
        self.__input_queue = name
        self.__output_exchange = output_exchange
        self.__middleware = QueueMiddleware()
        self.reducers_amount = reducers_amount
        self.state_log_file = "filter_by_three_stopovers/" + name + "_state_log.txt"
        self.flights_log_file = "filter_by_three_stopovers/" + name + "_flights_log.txt"
        self.__missing_flights = [set() for _ in range(NUMBER_CLIENTS)]
        self.eof_status = [False] * NUMBER_CLIENTS
        self.__accepted_flights = [0] * NUMBER_CLIENTS
        self.received = 0

    def run(self):
        signal.signal(signal.SIGTERM, self.__middleware.handle_sigterm)
        initialize_exchanges([self.__output_exchange],
                             self.__middleware)
        initialize_queues([self.__input_queue, self.__output_queue],
                          self.__middleware)
        self.recover_state_filters()
        self.__middleware.listen_on(self.__input_queue, self.callback)

    def callback(self, body, method):
        flight = json.loads(body)
        op_code = flight.get("op_code")
        message_id = flight.get("message_id")
        client_id = flight.get("client_id")
        index = int(client_id) - 1
        if message_id not in self.__missing_flights[index] and self.eof_status[index]:
            self.__middleware.manual_ack(method)
            return
        if op_code > FLIGHT_REGISTER:
            self.__middleware.manual_ack(method)
            return
        if op_code == EOF_FLIGHTS_FILE:
            log_to_file(self.state_log_file, f"{BEGIN_EOF},{message_id},{client_id},{self.__id}")
            self.eof_status[index] = True
            self.__accepted_flights[index] = get_missing_flights(self.flights_log_file,
                                                                 self.__missing_flights[index],
                                                                 self.__id,
                                                                 self.reducers_amount,
                                                                 message_id,
                                                                 client_id)
            self.send_and_log_eof(self.__accepted_flights[index],
                                  self.__id, client_id, message_id)
            self.__middleware.manual_ack(method)
            return
        filtering_result = self.filtering(flight)
        if self.eof_status[index]:
            if len(self.__missing_flights[index]) > 0:
                if filtering_result == ACCEPTED:
                    self.__accepted_flights[index] += 1
                self.__missing_flights[index].remove(message_id)
            self.send_and_log_eof(self.__accepted_flights[index], self.__id, client_id, message_id)
        log_to_file(self.flights_log_file, f"{message_id},{client_id},{filtering_result}")
        self.__middleware.manual_ack(method)

    def filtering(self, flight):
        stopovers = flight["segmentsArrivalAirportCode"].split("||")[:-1]
        filtering_result = FILTERED
        if len(stopovers) >= self.__max_stopovers:
            flight["stopovers"] = stopovers
            self.__middleware.publish(self.__output_exchange,
                                      json.dumps(flight))
            message = self.__create_message(flight)
            self.__middleware.send_message(self.__output_queue,
                                           json.dumps(message))
            filtering_result = ACCEPTED
        return filtering_result

    def __create_message(self, flight):
        message = dict()
        for i in range(len(self.__columns_to_filter)):
            message[self.__columns_to_filter[i]] = flight[self.__columns_to_filter[i]]
        message["result_id"] = f"{flight.get('message_id')}_{flight.get('client_id')}"
        return message

    def create_eof_flights_message(self, accepted_flights, filter_id, client_id):
        register = dict()
        register["op_code"] = EOF_FLIGHTS_FILE
        register["messages_sent"] = int(accepted_flights)
        register["client_id"] = int(client_id)
        register["filter_id"] = int(filter_id)
        return register

    def recover_state_filters(self):
        processed_clients = []
        if os.path.exists(self.state_log_file):
            with open(self.state_log_file, 'r+') as file:
                try:
                    lines = file.readlines()
                    for i, line in enumerate(lines):
                        if line.endswith("\n"):
                            line = line.strip('\n')
                            info, message_id, client_id, filter_id = tuple(line.split(","))
                            if int(info) == BEGIN_EOF:
                                client_id = int(client_id)
                                if client_id in processed_clients:
                                    continue
                                processed_clients.append(client_id)
                                index = client_id - 1
                                self.eof_status[index] = True
                                self.__accepted_flights[index] = get_missing_flights(self.flights_log_file,
                                                                                     self.__missing_flights[index],
                                                                                     self.__id,
                                                                                     self.reducers_amount,
                                                                                     int(message_id),
                                                                                     client_id)
                                self.send_and_log_eof(self.__accepted_flights[index], filter_id, client_id, message_id)
                        else:
                            file.truncate()
                            return
                except IndexError as e:
                    print(e)
                    return

    def send_and_log_eof(self, accepted_flights, filter_id, client_id, message_id):
        if len(self.__missing_flights[client_id - 1]) == 0:
            eof = self.create_eof_flights_message(accepted_flights, filter_id, client_id)
            eof["message_id"] = int(message_id)
            self.__middleware.publish(self.__output_exchange,
                                      json.dumps(eof))
            self.__middleware.send_message(self.__output_queue, json.dumps(eof))
            log_to_file(self.state_log_file, f"{EOF_SENT},{eof.get('client_id')}")
