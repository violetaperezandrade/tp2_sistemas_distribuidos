import json

from util.constants import EOF_FLIGHTS_FILE
from util.queue_middleware import QueueMiddleware


class FilterByThreeStopovers:
    def __init__(self, stopovers_column_name, columns_to_filter, max_stopovers,
                 output_queue, query_number, work_queue):
        self.__max_stopovers = max_stopovers
        self.__stopovers_column_name = stopovers_column_name
        self.__columns_to_filter = columns_to_filter
        self.__output_queue = output_queue
        self.__query_number = query_number
        self.__work_queue = work_queue
        self.middleware = QueueMiddleware()

    def run(self, input_exchange):
        self.middleware.subscribe_to(input_exchange,
                                     self.callback,
                                     "flights",
                                     self.__work_queue)

    def callback(self, body):
        flight = json.loads(body)
        op_code = flight.get("op_code")
        if op_code == EOF_FLIGHTS_FILE:
            remaining_nodes = flight.get("remaining_nodes")
            if remaining_nodes == 1:
                self.middleware.send_message_to(self.__output_queue, body)
                self.middleware.finish()
                return
            flight["remaining_nodes"] -= 1
            self.middleware.send_message_to(self.__work_queue,
                                            json.dumps(flight))
            self.middleware.finish()
            return
        stopovers = flight[self.__stopovers_column_name].split("||")[:-1]
        if len(stopovers) >= self.__max_stopovers:
            message = self.__create_message(flight,
                                            stopovers,
                                            self.__query_number)
            self.middleware.send_message_to(self.__output_queue,
                                            json.dumps(message))

    def __create_message(self, flight, stopovers, query_number):
        message = dict()
        for i in range(len(self.__columns_to_filter)):
            message[self.__columns_to_filter[i]] = (
                flight)[self.__columns_to_filter[i]]
        message["stopovers"] = stopovers
        message["queryNumber"] = query_number
        return message
