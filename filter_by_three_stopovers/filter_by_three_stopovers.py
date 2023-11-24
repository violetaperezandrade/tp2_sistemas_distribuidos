import json
import signal
import os
from random import randint
from time import sleep

from util.constants import EOF_FLIGHTS_FILE, FLIGHT_REGISTER, EOF_SENT, FILTERED, ACCEPTED, BEGIN_EOF
from util.file_manager import log_to_file
from util.initialization import initialize_exchanges, initialize_queues
from util.queue_middleware import QueueMiddleware
from util.recovery_logging import propagate_eof_standard, recover_state, message_duplicated, log_get_missing_flights


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
        self.state_log_filename = "filter_by_three_stopovers/" + name + ".txt"
        self.flights_log_filename = "filter_by_three_stopovers/" + name + "_flights.txt"
        self.__missing_flights = set()
        self.__client_receive_eof_status = [False]
        self.__accepted_flights = 0

    def run(self):
        signal.signal(signal.SIGTERM, self.__middleware.handle_sigterm)
        initialize_exchanges([self.__output_exchange],
                             self.__middleware)
        initialize_queues([self.__input_queue, self.__output_queue],
                          self.__middleware)
        # aca se podria checkpointear
        self.recover_state_filters(self.state_log_filename,
                                   self.flights_log_filename,
                                   self.__accepted_flights,
                                   self.reducers_amount,
                                   self.__id,
                                   self.__middleware,
                                   self.__missing_flights,
                                   self.__client_receive_eof_status,
                                   self.__output_queue)
        self.__middleware.listen_on(self.__input_queue, self.callback)

    def callback(self, body, method):
        flight = json.loads(body)
        op_code = flight.get("op_code")
        message_id = flight.get("message_id")
        client_id = flight.get("client_id")
        if message_id not in self.__missing_flights and self.__client_receive_eof_status[client_id - 1]:
            self.__middleware.manual_ack(method)
            return
        if op_code > FLIGHT_REGISTER:
            self.__middleware.manual_ack(method)
            return
        if op_code == EOF_FLIGHTS_FILE:
            messages_sent = flight["messages_sent"]
            log_to_file(self.state_log_filename, f"{BEGIN_EOF},{message_id},{messages_sent},{client_id},{self.__id}")
            self.__client_receive_eof_status[client_id - 1] = True
            self.__accepted_flights = log_get_missing_flights(self.flights_log_filename,
                                                              self.__missing_flights,
                                                              self.__id,
                                                              self.reducers_amount,
                                                              message_id)
            if len(self.__missing_flights) == 0:
                flight["filter_id"] = self.__id
                flight["messages_sent"] = self.__accepted_flights
                print(flight)
                self.__middleware.publish(self.__output_exchange,
                                          json.dumps(flight))
                self.__middleware.send_message(self.__output_queue, json.dumps(flight))
                log_to_file(self.state_log_filename, f"{EOF_SENT},{flight.get('client_id')},")
            self.__middleware.manual_ack(method)
            return
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

        if self.__client_receive_eof_status[client_id - 1]:
            if len(self.__missing_flights) > 0:
                if filtering_result == ACCEPTED:
                    self.__accepted_flights += 1
                self.__missing_flights.remove(message_id)
            if len(self.__missing_flights) == 0:
                register = self.create_eof_message(self.__accepted_flights,
                                                   self.__id,
                                                   client_id)
                register["message_id"] = -1
                self.__middleware.publish(self.__output_exchange,
                                          json.dumps(register))
                self.__middleware.send_message(
                    self.__output_queue, json.dumps(register))
                print(f"{register}")
                log_to_file(self.state_log_filename, f"{EOF_SENT},{flight.get('client_id')}")
        log_to_file(self.flights_log_filename, f"{message_id},{filtering_result}")
        self.__middleware.manual_ack(method)

    def __create_message(self, flight):
        message = dict()
        for i in range(len(self.__columns_to_filter)):
            message[self.__columns_to_filter[i]] = \
                flight[self.__columns_to_filter[i]]

        return message

    def create_eof_message(self, accepted_flights, filter_id, client_id):
        register = dict()
        register["op_code"] = 0
        register["messages_sent"] = accepted_flights
        register["client_id"] = client_id
        register["filter_id"] = filter_id
        return register

    def recover_state_filters(self, state_filename, flights_log_filename, accepted_flights, reducers_amount,
                              first_message, middleware, missing_flight_set,
                              eof_status, output_queue=None):
        missing_flight_set = set()
        accepted_flights = 0
        if os.path.exists(state_filename):
            with open(state_filename, 'r') as file:
                try:
                    last_line = file.readlines()[-1]
                except IndexError:
                    return
                if last_line.endswith("\n"):
                    last_line = last_line.strip('\n')
                    info = last_line.split(",")
                    if int(info[0]) == BEGIN_EOF:
                        message_id = info[1]
                        messages_sent = info[2]
                        client_id = info[3]
                        filter_id = info[4]
                        eof_status[int(client_id) - 1] = True
                        accepted_flights = log_get_missing_flights(flights_log_filename,
                                                                   missing_flight_set,
                                                                   first_message,
                                                                   reducers_amount,
                                                                   int(message_id))
                        if len(missing_flight_set) == 0:
                            register = self.create_eof_message(accepted_flights, filter_id, client_id)
                            register["message_id"] = -1
                            print(f"{register}")
                            self.__middleware.publish(self.__output_exchange,
                                                      json.dumps(register))
                            middleware.send_message(output_queue, json.dumps(register))
                            log_to_file(self.state_log_filename, f"{EOF_SENT},{register.get('client_id')},")
        return
