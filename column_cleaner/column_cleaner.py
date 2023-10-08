from util.queue_methods import (publish_on, acknowledge, send_message_to)
import json


class ColumnCleaner:
    def __init__(self, output_queue, output_exchange,
                 required_columns_flights, required_columns_airports,
                 ack_necessary):
        self.__output_queue = output_queue
        self.__output_exchange = output_exchange
        self.__required_columns_flights = required_columns_flights
        self.__required_columns_airports = required_columns_airports
        self.__ack_necessary = ack_necessary

    def callback(self, channel, method, properties, body):
        flight = json.loads(body)
        op_code = flight.get("op_code")
        if op_code == 0:
            self.__handle_eof(channel, method, body)
            return
        
        filtered_columns = dict()
        column_names = self.__required_columns_flights
        if flight["op_code"] == 2 and self.__required_columns_airports is not None:
            column_names = self.__required_columns_airports
        column_names.append("op_code")
        for column in column_names:
            filtered_columns[column] = flight[column]
        
        message = json.dumps(filtered_columns)
        if self.__output_exchange is not None:
            publish_on(channel, self.__output_exchange, message)
        else:
            send_message_to(channel, self.__output_queue, message)
        if self.__ack_necessary:
            acknowledge(channel, method)

    def __handle_eof(self, channel, method, body):
        if self.__output_exchange is not None:
            publish_on(channel, self.__output_exchange, body)
            acknowledge(channel, method)
        
        else:
            send_message_to(channel, self.__output_queue, body)