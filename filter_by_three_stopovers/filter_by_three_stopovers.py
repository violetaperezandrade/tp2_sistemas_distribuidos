import json
import signal
import os
from random import randint
from time import sleep

from util.constants import EOF_FLIGHTS_FILE, FLIGHT_REGISTER, EOF_SENT, FILTERED, ACCEPTED
from util.file_manager import log_to_file
from util.initialization import initialize_exchanges, initialize_queues
from util.queue_middleware import QueueMiddleware
from util.recovery_logging import propagate_eof_standard, recover_state, message_duplicated, log_get_missing_flights


class FilterByThreeStopovers:
    def __init__(self, columns_to_filter, max_stopovers,
                 output_queue, output_exchange, name,
                 reducers_amount):
        self.__max_stopovers = max_stopovers
        self.__columns_to_filter = columns_to_filter
        self.__output_queue = output_queue
        self.__input_queue = name
        self.__output_exchange = output_exchange
        self.__middleware = QueueMiddleware()
        self.reducers_amount = reducers_amount
        self.state_log_filename = "filter_by_three_stopovers/" + name + ".txt"
        self.flights_log_filename = "filter_by_three_stopovers/" + name + "_flights.txt"
        self.__missing_flights = set()

    def run(self):
        signal.signal(signal.SIGTERM, self.__middleware.handle_sigterm)
        initialize_exchanges([self.__output_exchange],
                             self.__middleware)
        initialize_queues([self.__input_queue, self.__output_queue],
                          self.__middleware)
        # aca se podria checkpointear
        recover_state(self.state_log_filename, self.__middleware, self.__output_queue)
        self.__middleware.listen_on(self.__input_queue, self.callback)

    def callback(self, body, method):
        flight = json.loads(body)
        op_code = flight.get("op_code")
        message_id = flight.get("message_id")
        if op_code > FLIGHT_REGISTER:
            self.__middleware.manual_ack(method)
            return
        if op_code == EOF_FLIGHTS_FILE:
            print(f"recibi eof {self.__input_queue}")
            self.__middleware.manual_ack(method)
            # ESCRIBO IDS EN LOG, DEJO PASAR TODO
            # log_get_missing_flights(self.flights_log_filename,
            #                         self.__missing_flights,
            #                         int(self.__input_queue[:-1]),
            #                         self.reducers_amount)
            # CUANDO ME LLEGA EOF, ITERO ARCHIVO, VEO LOS HUECOS (O SEA LOS MENSAJES QUE ME FALTAN).
            # CUANDO NO TENGO HUECOS TERMINE.
            return
        stopovers = flight["segmentsArrivalAirportCode"].split("||")[:-1]
        filtering_result = FILTERED
        if len(stopovers) >= self.__max_stopovers:
            flight["stopovers"] = stopovers
            self.__middleware.publish(self.__output_exchange,
                                      json.dumps(flight))
            message = self.__create_message(flight)
            self.__middleware.send_message(self.__output_queue,
                                           json.dumps(message))
            filtering_result = ACCEPTED
        log_to_file(self.state_log_filename, f"{message_id},{filtering_result}")
        self.__middleware.manual_ack(method)

    def __create_message(self, flight):
        message = dict()
        for i in range(len(self.__columns_to_filter)):
            message[self.__columns_to_filter[i]] = \
                flight[self.__columns_to_filter[i]]

        return message
