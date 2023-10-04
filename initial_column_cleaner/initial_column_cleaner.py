from util.queue_methods import (publish_on, acknowledge)
import json


class ColumnCleaner:
    def __init__(self, columns_names, output_queues):
        self.__columns_names = columns_names
        self.__output_queues = output_queues

    def callback(self, channel, method, properties, body):
        flight = json.loads(body)
        op_code = flight.get("op_code")
        if op_code == 0:
            # EOF
            for queue in self.__output_queues:
                publish_on(channel, queue, body)
                acknowledge(channel, method)
            return
        filtered_columns = dict()
        for column in self.__columns_names:
            filtered_columns[column] = flight[column]
        message = json.dumps(filtered_columns)
        for queue in self.__output_queues:
            publish_on(channel, queue, message)
            acknowledge(channel, method)
