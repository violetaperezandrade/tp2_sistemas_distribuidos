import json
from util.queue_middleware import (QueueMiddleware)


class FilterByAverage:
    def __init__(self, output_queue,input_queue, input_exchange):
        self.__output_queue = output_queue
        self.__input_queue = input_queue
        self.__input_exchange = input_exchange
        self.__middleware = QueueMiddleware()

    def run(self, avg_exchange):            
        self.__middleware.subscribe_without_consumption(self.__input_exchange, "flights", "cleaned_column_queue")

        self.__middleware.subscribe_to( avg_exchange, self.__callback_avg)    

        self.__middleware.listen_on("cleaned_column_queue", self.__callback_filter)
        
    
    def __callback_avg(self, body):
        self.__avg = json.loads(body)["avg"]
        print("avg received")
        self.__middleware.finish(True)

    def __callback_filter(self, body):
        flight = json.loads(body)
        op_code = flight.get("op_code")
        if op_code == 0:
            self.__handle_eof( channel, method, flight)
            return

        if float(flight["totalFare"]) > self.__avg:
            self.__middleware.publish_on(self.__output_queue, json.dumps(flight))
        

    def __handle_eof(self, flight):
        remaining_nodes = flight.get("remaining_nodes")
        if remaining_nodes == 1:
            print(f"eof received")
            self.__middleware.publish_on(channel, self.__output_queue, json.dumps(flight))
            self.__middleware.finish(True)
            return
        
        flight["remaining_nodes"] -= 1
        self.__middleware.send_message_to(self.__input_queue, json.dumps(flight))
        self.__middleware.finish(True)