from random import randint
from time import sleep

from util.constants import (EOF_FLIGHTS_FILE,
                            AIRPORT_REGISTER,
                            EOF_AIRPORTS_FILE,
                            FLIGHT_REGISTER,
                            BEGIN_EOF,
                            EOF_SENT)
import json
import signal
import os

from util.initialization import initialize_exchanges, initialize_queues
from util.queue_middleware import QueueMiddleware
from util.file_manager import log_to_file


class ColumnCleaner:
    def __init__(self, output_queue, output_exchange, input_queue,
                 required_columns_flights, required_columns_airports,
                 routing_key, connected_nodes, name):
        self.__output_queue = output_queue
        self.__output_exchange = output_exchange
        self.__input_queue = input_queue
        self.__required_columns_flights = required_columns_flights
        self.__required_columns_airports = required_columns_airports
        self.__routing_key = routing_key
        self.__connected_nodes = connected_nodes
        self.middleware = QueueMiddleware()
        self._filename = "column_cleaner/" + name + "_log_state.txt"

    def run(self, input_exchange):
        signal.signal(signal.SIGTERM, self.middleware.handle_sigterm)
        initialize_exchanges([self.__output_exchange, input_exchange],
                             self.middleware)
        initialize_queues([self.__output_queue, self.__input_queue],
                          self.middleware)
        self.__check_state()
        if input_exchange is not None:
            self.middleware.subscribe(input_exchange,
                                      self.callback,
                                      self.__input_queue)
        else:
            self.middleware.listen_on(self.__input_queue, self.callback)

    def callback(self, body, method):
        register = json.loads(body)
        op_code = register.get("op_code")
        if self.__routing_key == "flights" and op_code > FLIGHT_REGISTER:
            return
        if op_code == EOF_AIRPORTS_FILE:
            self.__output_message(body, op_code)
            self.middleware.manual_ack(method)
            return
        if op_code == EOF_FLIGHTS_FILE:
            log_to_file(self._filename, f"{BEGIN_EOF},{register.get('message_id')},{register.get('client_id')}")
            sleep_time = randint(15, 30)
            print(f"I am sleeping for {sleep_time}")
            sleep(sleep_time)
            self.middleware.manual_ack(method)
            print("Done sleeping")
            self.__output_message(body, op_code)
            print(f"I am sleeping for {sleep_time}")
            sleep(sleep_time)
            print("Done sleeping")
            log_to_file(self._filename, f"{EOF_SENT},{register.get('message_id')},"
                                        f"{register.get('client_id')}")
            print(f"I am sleeping for {sleep_time}")
            sleep(sleep_time)
            print("Done sleeping")
            return
        filtered_columns = dict()
        column_names = self.__required_columns_flights
        if register["op_code"] == AIRPORT_REGISTER:
            if self.__required_columns_airports != ['']:
                column_names = self.__required_columns_airports
            else:
                self.middleware.publish(self.__output_exchange, body)
                self.middleware.manual_ack(method)
                return
        for column in column_names:
            filtered_columns[column] = register[column]
        message = json.dumps(filtered_columns)
        self.__output_message(message, op_code)
        self.middleware.manual_ack(method)

    def __output_message(self, msg, op_code):
        if self.__output_exchange is not None:
            if self.__routing_key == "all" and op_code <= FLIGHT_REGISTER:
                self.middleware.send_message(self.__output_queue, msg)
                return
            self.middleware.publish(self.__output_exchange, msg)
        else:
            self.middleware.send_message(self.__output_queue, msg)

    def __check_state(self):
        if os.path.exists(self._filename):
            with open(self._filename, 'r') as file:
                try:
                    last_line = file.readlines()[-1]
                except IndexError:
                    return
                if last_line.endswith("\n"):
                    last_line = last_line.strip('\n')
                    op_code,message_id,client_id = tuple(last_line.split(','))
                    op_code = int(op_code)
                    # si es 1 -> ok
                    if op_code == EOF_SENT:
                        print("Recovered state, no need to send anything")
                        return
                    # si es 0 -> repetir pasos
                    msg = {"op_code": op_code,
                           "message_id": message_id,
                           "client_id": client_id}
                    self.__output_message(json.dumps(msg), op_code)
                    print("Recovered state, sending EOF")
                    log_to_file(self._filename, f"{EOF_SENT},{message_id},{client_id}")
                    print("Wrote EOF SENT to file")
        else:
            return
