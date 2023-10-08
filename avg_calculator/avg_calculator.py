import json
from util.queue_methods import (publish_on, send_message_to, acknowledge)


class AvgCalculator:
    def __init__(self, column_name, output_exchange,input_exchange, input_queue):
        self.__column_name = column_name
        self.__output_exchange = output_exchange
        self.__input_exchange = input_exchange
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
        elif op_code == 7:
            self.__handle_avg_res(channel, method, flight)
            return
        
        self.__result["sum"] += float(flight[self.__column_name])  
        self.__result["count"] += 1
        acknowledge(channel, method)
    
    def __handle_eof(self, channel, method, flight):
        remaining_nodes = flight.get("remaining_nodes")
        flight["sum"] = self.__result["sum"]
        flight["count"] = self.__result["count"]
        flight["op_code"] = 7
        
        if remaining_nodes == 1:
            print(f"remaining nodes: {remaining_nodes} result: {flight}")
            final_result = flight["sum"] / flight["count"]
            message = self.__generate_mesage(final_result)
            print(f"message: {message} queue: {self.__output_exchange}")
            
            publish_on(channel, self.__output_exchange, json.dumps(message))
            
            acknowledge(channel, method)
            return
        
        flight["remaining_nodes"] -= 1
        send_message_to(channel, self.__input_queue, json.dumps(flight))
        acknowledge(channel, method)
        channel.close()
        return


    def __handle_avg_res(self, channel, method, flight):
        remaining_nodes = flight.get("remaining_nodes")
        flight["sum"] += self.__result["sum"]
        flight["count"] += self.__result["count"]
        
        if remaining_nodes == 1:
            print(f"remaining nodes: {remaining_nodes} result: {flight}")
            final_result = flight["sum"] / flight["count"]
            message = self.__generate_mesage(final_result)
            print(f"message: {message} queue: {self.__output_exchange}")
            
            publish_on(channel, self.__output_exchange, json.dumps(message))
            
            acknowledge(channel, method)
            return
        
        flight["remaining_nodes"] -= 1
        send_message_to(channel, self.__input_queue, json.dumps(flight))
        acknowledge(channel, method)
        channel.close()
        return

    def __generate_mesage(self, avg):
        message = dict()
        message["op_code"] = 7
        message["avg"] = avg
        return message