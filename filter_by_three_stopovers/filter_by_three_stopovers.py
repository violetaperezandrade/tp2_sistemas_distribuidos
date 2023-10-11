import json
from util.constants import EOF_FLIGHTS_FILE, FLIGHT_REGISTER
from util.queue_middleware import QueueMiddleware


class FilterByThreeStopovers:
    def __init__(self, stopovers_column_name, columns_to_filter, max_stopovers,
                 output_queue, input_queue, output_exchange):
        self.__max_stopovers = max_stopovers
        self.__stopovers_column_name = stopovers_column_name
        self.__columns_to_filter = columns_to_filter
        self.__output_queue = output_queue
        self.__input_queue = input_queue
        self.__output_exchange = output_exchange
        self.middleware = QueueMiddleware()

    def run(self, input_exchange):
        self.middleware.create_exchange(input_exchange, "fanout")
        self.middleware.create_queue(self.__input_queue)
        self.middleware.subscribe(input_exchange,
                                  self.callback,
                                  self.__input_queue)

    def callback(self, body):
        flight = json.loads(body)
        op_code = flight.get("op_code")
        if op_code > FLIGHT_REGISTER:
            return
        if op_code == EOF_FLIGHTS_FILE:
            remaining_nodes = flight.get("remaining_nodes")
            if remaining_nodes == 1:
                self.middleware.send_message(self.__output_queue, body)
                self.middleware.publish(self.__output_exchange,body)
                self.middleware.finish()
                return
            flight["remaining_nodes"] -= 1
            self.middleware.send_message_to(self.__input_queue,
                                            json.dumps(flight))
            self.middleware.finish()
            return
        count = len(flight[self.__stopovers_column_name])
        if count >= 18:
            stopovers = flight[self.__stopovers_column_name].split('||')
            flight["stopovers"] = stopovers
            self.middleware.publish_on(self.__output_exchange,json.dumps(flight))
            message = self.__create_message(flight)
            self.middleware.send_message(self.__output_queue, json.dumps(message))

    def __create_message(self, flight):
        message = dict()
        for i in range(len(self.__columns_to_filter)):
            message[self.__columns_to_filter[i]
            ] = flight[self.__columns_to_filter[i]]

        return message
