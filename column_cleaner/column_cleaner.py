from util.constants import (EOF_FLIGHTS_FILE,
                            AIRPORT_REGISTER,
                            EOF_AIRPORTS_FILE,
                            FLIGHT_REGISTER, CLEANUP)
import json
import signal

from util.initialization import initialize_exchanges, initialize_queues
from util.queue_middleware import QueueMiddleware


class ColumnCleaner:
    def __init__(self, output_queue, output_exchange, input_queue,
                 required_columns_flights, required_columns_airports,
                 routing_key):
        self.__output_queue = output_queue
        self.__output_exchange = output_exchange
        self.__input_queue = input_queue
        self.__required_columns_flights = required_columns_flights
        self.__required_columns_airports = required_columns_airports
        self.__routing_key = routing_key
        self.middleware = QueueMiddleware()

    def run(self, input_exchange):
        signal.signal(signal.SIGTERM, self.middleware.handle_sigterm)
        initialize_exchanges([self.__output_exchange, input_exchange],
                             self.middleware)
        initialize_queues([self.__output_queue, self.__input_queue],
                          self.middleware)
        if input_exchange is not None:
            self.middleware.subscribe(input_exchange,
                                      self.callback,
                                      self.__input_queue)
        else:
            self.middleware.listen_on(self.__input_queue, self.callback)

    def callback(self, body, method):
        register = json.loads(body)
        op_code = register.get("op_code")
        if op_code == CLEANUP:
            #self.__output_message(body, op_code)
            self.middleware.manual_ack(method)
            return
        if self.__routing_key == "flights" and op_code > FLIGHT_REGISTER:
            self.middleware.manual_ack(method)
            return
        if op_code == EOF_AIRPORTS_FILE or op_code == EOF_FLIGHTS_FILE:
            self.__output_message(body, op_code)
            self.middleware.manual_ack(method)
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
            if self.__routing_key == "all" and (op_code <= FLIGHT_REGISTER or op_code == CLEANUP):
                self.middleware.send_message(self.__output_queue, msg)
                return
            self.middleware.publish(self.__output_exchange, msg)
        else:
            self.middleware.send_message(self.__output_queue, msg)
