import json
import signal
from util.constants import EOF_FLIGHTS_FILE, FLIGHT_REGISTER
from util.initialization import initialize_exchanges, initialize_queues
from util.queue_middleware import QueueMiddleware


class FilterByThreeStopovers:
    def __init__(self, columns_to_filter, max_stopovers,
                 output_queue, input_queue, output_exchange):
        self.__max_stopovers = max_stopovers
        self.__columns_to_filter = columns_to_filter
        self.__output_queue = output_queue
        self.__input_queue = input_queue
        self.__output_exchange = output_exchange
        self.__middleware = QueueMiddleware()

    def run(self, input_exchange):
        signal.signal(signal.SIGTERM, self.__middleware.handle_sigterm)

        initialize_exchanges([input_exchange, self.__output_exchange], self.__middleware)
        initialize_queues([self.__input_queue, self.__output_queue], self.__middleware)
        self.__middleware.subscribe(input_exchange,
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
                self.__middleware.send_message(self.__output_queue, body)
                self.__middleware.publish(self.__output_exchange, body)
                self.__middleware.finish()
                return
            flight["remaining_nodes"] -= 1
            self.__middleware.send_message(self.__input_queue,
                                           json.dumps(flight))
            self.__middleware.finish()
            return
        stopovers = flight["segmentsArrivalAirportCode"].split("||")[:-1]
        if len(stopovers) >= self.__max_stopovers:
            flight["stopovers"] = stopovers
            self.__middleware.publish(self.__output_exchange, json.dumps(flight))
            message = self.__create_message(flight)
            self.__middleware.send_message(self.__output_queue, json.dumps(message))

    def __create_message(self, flight):
        message = dict()
        for i in range(len(self.__columns_to_filter)):
            message[self.__columns_to_filter[i]
            ] = flight[self.__columns_to_filter[i]]

        return message
