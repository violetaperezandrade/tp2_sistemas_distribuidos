import json
import re
from util.queue_methods import (send_message_to, acknowledge,
                                connect_mom, listen_on, create_queue)


class FastestsCalculator():
    def __init__(self, grouped_by_queue, output_queue,
                 reducers_amount, result_fields, duration_field):
        self.grouped_by_queue = grouped_by_queue
        self.output_queue = output_queue
        self.reducers_amount = reducers_amount
        self.result_fields = result_fields
        self.duration_field = duration_field
        self.nodes_finished = 0

    def run(self):
        connection = connect_mom()
        channel = connection.channel()

        create_queue(channel, self.grouped_by_queue)
        create_queue(channel, self.output_queue)

        listen_on(channel, self.grouped_by_queue, self.__callback)

        channel.close()
        connection.close()

    def __callback(self, channel, method, properties, body):
        flight = json.loads(body)
        op_code = flight.get("op_code", "")

        if op_code == 0:
            # EOF
            if self.nodes_finished == self.reducers_amount:
                send_message_to(channel, self.output_queue, body)
                acknowledge(channel, method)
                return
            else:
                self.nodes_finished += 1
                acknowledge(channel, method)
                return
        fastest_flights = [{}, {}]
        for flight in list(flight.values())[0]:
            duration = self.__convert_duration(flight[self.duration_field])
            if fastest_flights[0] == {} or duration < fastest_flights[0].get('duration'):
                fastest_flights[1] = fastest_flights[0]
                fastest_flights[0] = self.__get_result(flight, duration)
            elif fastest_flights[1] == {} or duration < fastest_flights[1].get('duration'):
                fastest_flights[1] = self.__get_result(flight, duration)
        send_message_to(channel, self.output_queue, json.dumps(fastest_flights))
        acknowledge(channel, method)
        return

    def __convert_duration(self, duration_str):
        match = re.match(r'PT(\d+)H(\d+)M', duration_str)

        if match:
            hours = int(match.group(1))
            minutes = int(match.group(2))
            total_minutes = hours * 60 + minutes
            return total_minutes
        else:
            return None  # Handle

    def __get_result(self, flight, duration):
        result = {}
        for field in self.result_fields:
            result[field] = flight[field]
        result["duration"] = duration
        return result
