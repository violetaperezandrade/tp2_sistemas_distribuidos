import json
from util.queue_methods import (send_message_to, acknowledge)


class FilterByThreeStopovers:
    def __init__(self, stopovers_column_name, columns_to_filter, max_stopovers,
                 output_queue, query_number):
        self.__max_stopovers = max_stopovers
        self.__stopovers_column_name = stopovers_column_name
        self.__columns_to_filter = columns_to_filter
        self.__output_queue = output_queue
        self.__query_number = query_number

    def callback(self, channel, method, properties, body):
        flight = json.loads(body)
        op_code = flight.get("op_code")
        if op_code == 0:
            # EOF
            print("EOF !!!")
            send_message_to(channel, self.__output_queue, body)
            acknowledge(channel, method)
            return
        stopovers = flight[self.__stopovers_column_name].split("||")[:-1]
        if len(stopovers) >= self.__max_stopovers:
            # Publish on query 3's queue here
            message = self.__create_message(
                flight, stopovers, self.__query_number)
            send_message_to(channel, self.__output_queue, json.dumps(message))
            acknowledge(channel, method)

    def __create_message(self, flight, stopovers, query_number):
        message = dict()
        for i in range(len(self.__columns_to_filter)):
            message[self.__columns_to_filter[i]
                    ] = flight[self.__columns_to_filter[i]]

        message["stopovers"] = stopovers
        message["queryNumber"] = query_number

        return message
