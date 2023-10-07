import json
from util.queue_methods import (send_message_to, acknowledge)


class FilterByAverage:
    def __init__(self, output_queue,input_queue):
        self.__output_queue = output_queue
        self.__input_queue = input_queue
        self.avg_recieved = False

    def callback(self, channel, method, properties, body):
        print(json.loads(body))
        self.avg_recieved = True
        channel.close()
