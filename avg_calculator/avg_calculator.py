import json
from util.queue_methods import (send_message_to, acknowledge)


class AvgCalculator:
    def __init__(self, column_name, output_queue,input_queue):
        self.__column_name = column_name
        self.__output_queue = output_queue
        self.__input_queue = input_queue
        self.__result = dict()
        self.__result["sum"] = 0.0
        self.__result["count"] = 0

    def callback(self, channel, method, properties, body):
        flight = json.loads(body)
        op_code = flight.get("op_code")
        if op_code == 0:
            self.__handle_eof( channel, method, flight)
            return

        self.__result["sum"] += float(flight[self.__column_name])  
        self.__result["count"] += 1  
    
    def __handle_eof(self, channel, method, flight):
        remaining_nodes = flight.get("remaining_nodes")
        if flight.get("sum") == None:
            flight["sum"] = 0
            flight["count"] = 0
        
        flight["sum"] += self.__result["sum"]
        flight["count"] += self.__result["count"]
        
        if remaining_nodes == 1:
            # send_message_to(channel, self.__output_queue, body)
            # acknowledge(channel, method)
            print(f"remaining nodes: {remaining_nodes} result: {flight}")
            final_result = flight["sum"] / flight["count"]
            message = self.__generate_mesage(final_result)
            print(f"message: {message} queue: {self.__output_queue}")
            send_message_to(channel, self.__output_queue, json.dumps(message))
            acknowledge(channel, method)
            channel.close()
            return
        
        flight["remaining_nodes"] -= 1
        print(flight)
        send_message_to(channel, self.__input_queue, json.dumps(flight))
        acknowledge(channel, method)
        channel.close()
        return

    def __generate_mesage(self, avg):
        message = dict()
        message["op_code"] = 1
        message["avg"] = avg
        return message