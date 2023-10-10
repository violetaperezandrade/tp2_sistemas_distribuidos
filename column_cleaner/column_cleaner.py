from util.constants import (EOF_FLIGHTS_FILE,
                            AIRPORT_REGISTER,
                            EOF_AIRPORTS_FILE)
import json

from util.queue_middleware import QueueMiddleware


class ColumnCleaner:
    def __init__(self, output_queue, output_exchange,
                 required_columns_flights, required_columns_airports,
                 routing_key):
        self.__output_queue = output_queue
        self.__output_exchange = output_exchange
        self.__required_columns_flights = required_columns_flights
        self.__required_columns_airports = required_columns_airports
        self.__routing_key = routing_key
        self.middleware = QueueMiddleware()

    def run(self, input_exchange, input_queue):
        if input_exchange is not None:
            self.middleware.subscribe_to(input_exchange,
                                         self.callback,
                                         self.__routing_key,
                                         input_queue)
        else:
            self.middleware.listen_on(input_queue, self.callback)

    def callback(self, body):
        register = json.loads(body)
        op_code = register.get("op_code")
        if op_code == EOF_FLIGHTS_FILE or op_code == EOF_AIRPORTS_FILE:
            self.__output_message(body, op_code)
            return
        
        filtered_columns = dict()
        column_names = self.__required_columns_flights
        if register["op_code"] == AIRPORT_REGISTER:
            if self.__required_columns_airports != ['']:
                column_names = self.__required_columns_airports
            else:
                self.middleware.publish_on(self.__output_exchange,
                                           body,
                                           "airports")
                return
        column_names.append("op_code")
        for column in column_names:
            filtered_columns[column] = register[column]
        message = json.dumps(filtered_columns)
        self.__output_message(message, op_code)

    def __output_message(self, msg, op_code):
        if self.__output_exchange is not None:
            routing = 'flights'
            if op_code == AIRPORT_REGISTER or op_code == EOF_AIRPORTS_FILE:
                routing = 'airports'
            else:
                if self.__routing_key == "#":
                    self.middleware.send_message_to(self.__output_queue, msg)
                    return
            self.middleware.publish_on(self.__output_exchange, msg, routing)
        else:
            self.middleware.send_message_to(self.__output_queue, msg)
