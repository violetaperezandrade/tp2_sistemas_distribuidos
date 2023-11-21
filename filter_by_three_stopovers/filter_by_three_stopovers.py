import json
import signal
import os
from util.constants import EOF_FLIGHTS_FILE, FLIGHT_REGISTER, EOF_SENT
from util.initialization import initialize_exchanges, initialize_queues
from util.queue_middleware import QueueMiddleware
from util.file_manager import log_to_file


class FilterByThreeStopovers:
    def __init__(self, columns_to_filter, max_stopovers,
                 output_queue, input_queue, output_exchange, name):
        self.__max_stopovers = max_stopovers
        self.__columns_to_filter = columns_to_filter
        self.__output_queue = output_queue
        self.__input_queue = input_queue
        self.__output_exchange = output_exchange
        self.__middleware = QueueMiddleware()
        self._filename = "filter_by_three_stopovers/" + name + ".txt"

    def run(self, input_exchange):
        signal.signal(signal.SIGTERM, self.__middleware.handle_sigterm)

        initialize_exchanges([input_exchange, self.__output_exchange],
                             self.__middleware)
        initialize_queues([self.__input_queue, self.__output_queue],
                          self.__middleware)
        self.__check_state()
        self.__middleware.subscribe(input_exchange,
                                    self.callback,
                                    self.__input_queue)

    def callback(self, body, method):
        flight = json.loads(body)
        op_code = flight.get("op_code")

        if op_code > FLIGHT_REGISTER:
            return
        if op_code == EOF_FLIGHTS_FILE:
            log_to_file(self._filename, f"0,{flight.get('message_id')},"
                        f"{flight.get('client_id')}")
            self.__middleware.manual_ack(method)
            self.__middleware.send_message(self.__output_queue, body)
            log_to_file(self._filename, f"1,{flight.get('message_id')},"
                        f"{flight.get('client_id')}")
            return
        stopovers = flight["segmentsArrivalAirportCode"].split("||")[:-1]
        if len(stopovers) >= self.__max_stopovers:
            flight["stopovers"] = stopovers
            self.__middleware.publish(self.__output_exchange,
                                      json.dumps(flight))
            message = self.__create_message(flight)
            self.__middleware.send_message(self.__output_queue,
                                           json.dumps(message))
            self.__middleware.manual_ack(method)

    def __create_message(self, flight):
        message = dict()
        for i in range(len(self.__columns_to_filter)):
            message[self.__columns_to_filter[i]] = \
                flight[self.__columns_to_filter[i]]

        return message

    def __check_state(self):
        if os.path.exists(self._filename):
            with open(self._filename, 'r') as file:
                try:
                    last_line = file.readlines()[-1]
                except IndexError:
                    return
                if last_line.endswith("\n"):
                    last_line.strip("\n")
                    op_code, message_id, client_id = tuple(last_line.split(','))
                    op_code = int(op_code)
                    # si es 1 -> ok
                    if op_code == EOF_SENT:
                        print("Recovered state, no need to send anything")
                        return
                    # si es 0 -> repetir pasos
                    msg = {"op_code": op_code,
                           "message_id": message_id,
                           "client_id": client_id}
                    self.__middleware.send_message(self.__output_queue,
                                                   json.dumps(msg))
                    print("Recovered state, sending EOF")
                    log_to_file(self._filename, f"{EOF_SENT}, {message_id}, {client_id}")
        else:
            return
