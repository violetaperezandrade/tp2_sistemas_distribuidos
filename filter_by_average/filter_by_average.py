import json
from util.queue_methods import (send_message_to, acknowledge)


class FilterByAverage:
    def __init__(self, output_queue,input_queue):
        self.__output_queue = output_queue
        self.__input_queue = input_queue
        self.avg_recieved = False

    def callback_avg(self, channel, method, properties, body):
        print(json.loads(body))
        self.__avg = json.loads(body)["avg"]    
        self.avg_recieved = True
        channel.close()

    def callback_filter(self, channel, method, properties, body):
        flight = json.loads(body)
        if float(flight["totalFare"]) > self.__avg:
            print(f"{flight}")
