from util.constants import EOF_FLIGHTS_FILE, AIRPORT_REGISTER
import json

from util.queue_middleware import QueueMiddleware


class ColumnCleaner:
    def __init__(self, output_queue, output_exchange,
                 required_columns_flights, required_columns_airports):
        self.__output_queue = output_queue
        self.__output_exchange = output_exchange
        self.__required_columns_flights = required_columns_flights
        self.__required_columns_airports = required_columns_airports
        self.__queue_middleware = QueueMiddleware()

    def run(self, input_exchange, input_queue):
        if input_exchange is not None:
            self.__queue_middleware.subscribe_to(input_exchange, self.callback)
        else:
            self.__queue_middleware.listen_on(input_queue, self.callback)

    def callback(self, body):
        flight = json.loads(body)
        op_code = flight.get("op_code")
        if op_code == EOF_FLIGHTS_FILE:
            if self.__output_exchange is not None:
                self.__queue_middleware.publish_on(self.__output_exchange, body)
            else:
                self.__queue_middleware.send_message_to(self.__output_queue, body)
            return
        filtered_columns = dict()
        column_names = self.__required_columns_flights
        if flight["op_code"] == AIRPORT_REGISTER and self.__required_columns_airports is not None:
            column_names = self.__required_columns_airports
        column_names.append("op_code")
        for column in column_names:
            filtered_columns[column] = flight[column]
        message = json.dumps(filtered_columns)
        if self.__output_exchange is not None:
            self.__queue_middleware.publish_on(self.__output_exchange, message)
        else:
            self.__queue_middleware.send_message_to(self.__output_queue, message)
