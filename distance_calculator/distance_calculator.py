import json

from util.constants import EOF_AIRPORTS_FILE, EOF_FLIGHTS_FILE
from util.queue_middleware import QueueMiddleware
from geopy.distance import geodesic


class DistanceCalculator:

    def __init__(self, input_exchange, input_queue, output_queue):
        self.__middleware = QueueMiddleware()
        self.__input_exchange = input_exchange
        self.__input_queue = input_queue
        self.__output_queue = output_queue
        self.__airports_distances = {}

    def run(self):
        self.__middleware.subscribe_to(self.__input_exchange,
                                       self.__airport_callback)
        self.__middleware.listen_on(self.__input_queue,
                                    self.__flight_callback)

    def __airport_callback(self, body):
        register = json.loads(body)
        if register["op_code"] == EOF_AIRPORTS_FILE:
            self.__middleware.finish(True)
            return
        self.__store_value(register)

    def __flight_callback(self, body):
        register = json.loads(body)
        if register["op_code"] == EOF_FLIGHTS_FILE:
            self.__middleware.finish()
            return
        self.__calculate_total_distance(register)
        if register["totalTravelDistance"] > float(4 * register["directDistance"]):
            register.pop('segmentsArrivalAirportCode', None)
            register.pop('directDistance', None)
            register.pop('op_code', None)
            register = json.dumps(register)
            self.__middleware.send_message_to(self.__output_queue, register)

    def __store_value(self, register):
        coordinates = (register["Latitude"], register["Longitude"])
        self.__airports_distances[register["Airport Code"]] = coordinates

    def __calculate_total_distance(self, register):
        stops = register["segmentsArrivalAirportCode"].split("||")
        stops.insert(0, register["startingAirport"])
        register["directDistance"] = self.__calculate_distance(register["startingAirport"],
                                                               register["destinationAirport"])
        if register["totalTravelDistance"] != '':
            distance_float = float(register["totalTravelDistance"])
            register["totalTravelDistance"] = distance_float
            return
        distance = 0
        for i in range(len(stops) - 1):
            print(stops[i])
            print(stops[i + 1])
            distance += self.__calculate_distance(stops[i],
                                                  stops[i + 1])
        register["totalTravelDistance"] = distance

    def __calculate_distance(self, start, end):
        coordinates_start = self.__airports_distances[start]
        coordinates_end = self.__airports_distances[end]
        return (geodesic(coordinates_start, coordinates_end)).miles
