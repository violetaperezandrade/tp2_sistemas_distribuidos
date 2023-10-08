import json
from util.queue_methods import (send_message_to, acknowledge, publish_on)


class FilterByThreeStopovers:
    def __init__(self, stopovers_column_name, columns_to_filter, max_stopovers,
                 output_queue, query_number, input_queue, output_exchange):
        self.__max_stopovers = max_stopovers
        self.__stopovers_column_name = stopovers_column_name
        # TODO: this node should only filter by stopovers
        self.__columns_to_filter = columns_to_filter
        self.__output_queue = output_queue
        self.__query_number = query_number
        self.__input_queue = input_queue
        self.__output_exchange = output_exchange

    def callback(self, channel, method, properties, body):
        flight = json.loads(body)
        op_code = flight.get("op_code")
        if op_code == 0:
            # EOF
            remaining_nodes = flight.get("remaining_nodes")
            if remaining_nodes == 1:
                send_message_to(channel, self.__output_queue, body)
                acknowledge(channel, method)
                eof = {"op_code": 0}
                publish_on(channel, self.__output_exchange, json.dumps(eof))
                channel.close()
                return
            flight["remaining_nodes"] -= 1
            send_message_to(channel, self.__input_queue, json.dumps(flight))
            acknowledge(channel, method)
            channel.close()
            return

        stopovers = flight[self.__stopovers_column_name].split("||")[:-1]
        if len(stopovers) >= self.__max_stopovers:
            # Publish on query 3's queue here
            # TODO: refactor
            message = self.__create_message(
                flight, stopovers, self.__query_number)
            publish_on(channel, self.__output_exchange, json.dumps(message))
        acknowledge(channel, method)

    def __create_message(self, flight, stopovers, query_number):
        message = dict()
        for i in range(len(self.__columns_to_filter)):
            message[self.__columns_to_filter[i]
                    ] = flight[self.__columns_to_filter[i]]

        message["stopovers"] = stopovers
        message["queryNumber"] = query_number

        return message
