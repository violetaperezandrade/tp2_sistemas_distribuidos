import json
from util.queue_methods import (send_message_to, acknowledge)


class FourthQueryHandler:
    def __init__(self, input_queue, output_queue):
        self.__output_queue = output_queue
        self.__input_queue = input_queue

    def callback(self, channel, method, properties, body):
        flight = json.loads(body)
        print(flight)
        op_code = flight.get("op_code")
        if op_code == 0:
            self.__handle_eof( channel, method, flight)
            print("eof")
            return
        # message = dict()
        # message["route"] = flight.keys()[0]
        # message["avg"] = flight["route"]["avg"]
        # message["sum"] = flight["route"]["sum"]
        # send_message_to(channel, self.__output_queue, json.dumps(message))
        
    
    def __handle_eof(self, channel, method, flight):
        pass
