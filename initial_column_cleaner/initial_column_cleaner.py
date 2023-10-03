from util.queue_methods import (publish_on, acknowledge)
import json


class ColumnCleaner:
    def __init__(self, columns_names, output_queues):
        self.__columns_names = columns_names
        self.__output_queues = output_queues

    def callback(self, channel, method, properties, body):
        if body.startswith(b'00'):
            pass
            # EOF
        filtered_byte_array = bytearray()
        filtered_byte_array += body[:1]
        body = body[3:].decode('utf-8')
        body_dict = json.loads(body)
        filtered_columns = dict()
        for column in self.__columns_names:
            filtered_columns[column] = body_dict[column]
        message = json.dumps(filtered_columns)
        for queue in self.__output_queues:
            publish_on(channel, queue, message)
            acknowledge(channel, method)
