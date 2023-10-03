from util.queue_methods import (publish_on,acknowledge)
import json

class ColumnCleaner:
    def __init__(self,columns, field_len, columns_name, indexes_needed, output_queues ):
        self.__columns = columns
        self.__field_len = field_len
        self.__columns_name = columns_name
        self.__indexes_needed = indexes_needed
        self.__output_queues = output_queues
    
    def callback(self, channel, method, properties, body):
        if body.startswith(b'00'):
            pass
            # EOF
        filtered_byte_array = bytearray()
        filtered_byte_array += body[:1]
        body = body[3:]
        bytes_readed = 0
        filtered_columns = dict()
        j = 0
        for i in range(1, self.__columns + 1):
            column_len = int.from_bytes(
                body[bytes_readed:bytes_readed + self.__field_len], byteorder="big")
            bytes_readed += self.__field_len
            column_data = body[bytes_readed:bytes_readed + column_len]
            if str(i) in self.__indexes_needed:
                filtered_columns[self.__columns_name[j]] = column_data.decode("utf-8")
                j += 1
            bytes_readed += column_len
        message = json.dumps(filtered_columns)
        for queue in self.__output_queues:
            publish_on(channel, queue, message)
            acknowledge(channel, method)

