import json
from hashlib import sha256
from util.queue_methods import (send_message_to, acknowledge,
                                connect_mom, subscribe_to, create_queue)


class GroupBy():
    def __init__(self, fields_group_by, listening_queue,
                 reducers_amount, queue_group_by, input_queue):
        self.listening_queue = listening_queue
        self.reducers_amount = reducers_amount
        self.reducers = [
            f"{queue_group_by}_{i}" for i in range(1, reducers_amount+1)]
        self.input_queue = input_queue
        self.hanlde_group_by_fields(fields_group_by)

    def hanlde_group_by_fields(self, fields_group_by):
        if len(fields_group_by) > 1:
            self.fields_list = fields_group_by
            self.field_group_by = 'route'
        else:
            self.fields_list = None
            self.field_group_by = fields_group_by[0]

    def run(self):
        connection = connect_mom()
        channel = connection.channel()

        for reducer in self.reducers:
            create_queue(channel, reducer)
        subscribe_to(channel, self.listening_queue, self.__callback,
                     self.input_queue)

        channel.start_consuming()
        channel.close()
        connection.close()

    def __callback(self, channel, method, properties, body):
        flight = json.loads(body)
        op_code = flight.get("op_code")

        if op_code == 0:
            # EOF
            for reducer in self.reducers:
                send_message_to(channel, reducer, body)
            acknowledge(channel, method)
            return
        if self.fields_list is not None:
            flight[self.field_group_by] = self.__create_route(flight,
                                                              self.fields_list)
        else:
            self.field_group_by = self.field_group_by[0]
        output_queue = self.__get_output_queue(flight, self.field_group_by)
        send_message_to(channel, self.reducers[output_queue],
                        json.dumps(flight))
        acknowledge(channel, method)

    def __create_route(self, flight, fields_list):
        return ("-").join(flight[str(field)] for field in fields_list)

    def __get_output_queue(self, flight, field_group_by):
        field = flight.get(field_group_by)
        hashed_field = int(sha256(field.encode()).hexdigest(), 16)
        return hashed_field % self.reducers_amount
