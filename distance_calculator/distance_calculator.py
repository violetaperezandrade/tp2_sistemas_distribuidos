import json

from util.constants import AIRPORT_REGISTER, EOF_AIRPORTS_FILE, EOF_FLIGHTS_FILE
from util.queue_middleware import QueueMiddleware
from geopy.distance import geodesic


class DistanceCalculator:

    def __init__(self, input_queue, output_queue):
        self.__middleware = QueueMiddleware()
        self.__input_queue = input_queue
        self.__output_queue = output_queue
        self.__airports_parsed = False
        self.__airports_distances = {}

    def run(self):
        self.__middleware.listen_on(self.__input_queue, self.__callback)

    def __callback(self, body):
        register = json.loads(body)
        print(register)
        if register["op_code"] == EOF_AIRPORTS_FILE:
            self.__airports_parsed = True
            return
        if register["op_code"] == AIRPORT_REGISTER:
            self.__store_value(register)
            return
        if register["op_code"] == EOF_FLIGHTS_FILE:
            return
        if not self.__airports_parsed:
            self.__middleware.send_message_to(self.__input_queue, body)
            return
        self.__calculate_total_distance(register)
        self.__middleware.send_message_to(self.__output_queue, json.dumps(register))

    def __store_value(self, register):
        coordinates = (register["Latitude"], register["Longitude"])
        self.__airports_distances[register["Airport Code"]] = coordinates

    def __calculate_total_distance(self, register):
        if register["totalTravelDistance"] != '':
            return
        print("necesario calcular")
        stops = register["segmentsArrivalAirportCode"].split("||")
        stops.insert(0, register["startingAirport"])
        distance = 0
        for i in range (len(stops)-1):
            distance += self.__calculate_distance(stops[i],stops[i+1])
        register["totalTravelDistance"] = distance
        print(distance)

    def __calculate_distance(self, start, end):
        coordinates_start = self.__airports_distances[start]
        coordinates_end = self.__airports_distances[end]
        return (geodesic(coordinates_start, coordinates_end)).km
