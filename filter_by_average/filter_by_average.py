import json
from util.queue_methods import (send_message_to, acknowledge, publish_on)


class FilterByAverage:
    def __init__(self, output_queue,input_queue, input_exchange):
        self.__output_queue = output_queue
        self.__input_queue = input_queue
        self.__input_exchange = input_exchange

    def callback_avg(self, channel, method, properties, body):
        self.__avg = json.loads(body)["avg"]
        print("avg received")
        channel.close()

    def callback_filter(self, channel, method, properties, body):
        flight = json.loads(body)
        print(flight)
        op_code = flight.get("op_code")
        if op_code == 0:
            self.__handle_eof( channel, method, flight)
            return

        # if float(flight["totalFare"]) > self.__avg:
        #     # print(f"{flight} selected")
        
        acknowledge(channel, method)

    def __handle_eof(self, channel, method, flight):
        remaining_nodes = flight.get("remaining_nodes")
        if remaining_nodes == 1:
            print(f"eof received")
            channel.close()
            return
        
        flight["remaining_nodes"] -= 1
        send_message_to(channel, self.__input_queue, json.dumps(flight))
        acknowledge(channel, method)
        channel.close()