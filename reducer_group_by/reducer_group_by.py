import json
from util.queue_methods import (send_message_to, acknowledge,
                                connect_mom, listen_on, create_queue)


class ReducerGroupBy():

    def __init__(self, field_group_by, input_queue, output_queue):
        self.field_group_by = field_group_by
        self.output_queue = output_queue
        self.grouped = {}
        self.input_queue = input_queue

    def run(self):
        connection = connect_mom()
        channel = connection.channel()

        create_queue(channel, self.input_queue)
        create_queue(channel, self.output_queue)

        listen_on(channel, self.input_queue, self.__callback)

        channel.close()
        connection.close()

    def __callback(self, channel, method, properties, body):
        flight = json.loads(body)
        op_code = flight.get("op_code")

        if op_code == 0:
            # EOF
            self.__handle_eof(channel)
            send_message_to(channel, self.output_queue, body)
            acknowledge(channel, method)
            return

        flight_group_by_field = flight[self.field_group_by]
        self.grouped[flight_group_by_field] = self.grouped.get(
            flight_group_by_field, [])
        self.grouped[flight_group_by_field].append(flight)
        acknowledge(channel, method)

    def __handle_eof(self, channel):
        for route, flights in self.grouped.items():
            # TODO: pienso que aca podriamos usar batches?
            msg = {route: flights}
            send_message_to(channel, self.output_queue, json.dumps(msg))
