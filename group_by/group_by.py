import json
from hashlib import sha256
from util.queue_middleware import QueueMiddleware
from util.constants import *


class GroupBy():
    def __init__(self, fields_group_by, input_exchange,
                 reducers_amount, queue_group_by, input_queue):
        self.queue_middleware = QueueMiddleware()
        self.input_exchange = input_exchange
        self.reducers_amount = reducers_amount
        self.reducers = [
            f"{queue_group_by}_{i}" for i in range(1, reducers_amount+1)]
        self.input_queue = input_queue
        self.handle_group_by_fields(fields_group_by)

    def handle_group_by_fields(self, fields_group_by):
        if len(fields_group_by) > 1:
            self.fields_list = fields_group_by
            self.field_group_by = 'route'
        else:
            self.fields_list = None
            self.field_group_by = fields_group_by[0]

    def run(self):

        self.queue_middleware.subscribe_to(self.input_exchange, self.__callback,
                     queue=self.input_queue)

    def __callback(self, body):
        flight = json.loads(body)
        op_code = flight.get("op_code")
        if op_code == EOF_FLIGHTS_FILE:
            print("Received eof")
            # EOF
            for reducer in self.reducers:
                self.queue_middleware.send_message_to(reducer, body)
            self.queue_middleware.finish()
            return
        if self.fields_list is not None:
            flight[self.field_group_by] = self.__create_route(flight,
                                                              self.fields_list)
        else:
            self.field_group_by = self.field_group_by[0]
        output_queue = self.__get_output_queue(flight, self.field_group_by)
        self.queue_middleware.send_message_to(self.reducers[output_queue],
                                              json.dumps(flight))

    def __create_route(self, flight, fields_list):
        return ("-").join(flight[str(field)] for field in fields_list)

    def __get_output_queue(self, flight, field_group_by):
        field = flight.get(field_group_by)
        hashed_field = int(sha256(field.encode()).hexdigest(), 16)
        return hashed_field % self.reducers_amount
