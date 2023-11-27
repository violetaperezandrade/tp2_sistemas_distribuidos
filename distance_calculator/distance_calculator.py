import json
from geopy.distance import geodesic
import signal

from util.constants import EOF_AIRPORTS_FILE, EOF_FLIGHTS_FILE, NUMBER_CLIENTS
from util.initialization import initialize_exchanges, initialize_queues
from util.queue_middleware import QueueMiddleware


class DistanceCalculator:

    def __init__(self, input_exchange, input_queue, output_queue, pipe):
        self.__middleware = QueueMiddleware()
        self.__airports_distances = {key: dict() for key in range(1, NUMBER_CLIENTS + 1)}
        self.__input_exchange = input_exchange
        self.__input_queue = input_queue
        self.__output_queue = output_queue
        self.__pipe = pipe

    def run(self):
        signal.signal(signal.SIGTERM, self.__middleware.handle_sigterm)
        initialize_exchanges([self.__input_exchange], self.__middleware)
        initialize_queues([self.__input_queue, self.__output_queue],
                          self.__middleware)
        self.__middleware.listen_on(self.__input_queue,
                                    self.__flight_callback)

    def __flight_callback(self, body, method):
        register = json.loads(body)
        client_id = register["client_id"]
        if register["op_code"] == EOF_FLIGHTS_FILE:
            self.__middleware.send_message(self.__output_queue,
                                           json.dumps(register))
            self.__middleware.manual_ack(method)
            return
        self.__calculate_total_distance(register, client_id)
        if register["totalTravelDistance"] > 4 * register["directDistance"]:
            register.pop('segmentsArrivalAirportCode', None)
            register.pop('directDistance', None)
            register.pop('op_code', None)
            register = json.dumps(register)
            self.__middleware.send_message(self.__output_queue, register)
        self.__middleware.manual_ack(method)

    def __calculate_total_distance(self, register, client_id):
        self.get_correct_dictionary(client_id)
        stops = register["segmentsArrivalAirportCode"].split("||")
        stops.insert(0, register["startingAirport"])
        register["directDistance"] = self.__calculate_distance(
            register["startingAirport"],
            register["destinationAirport"],
            client_id)
        if register["totalTravelDistance"] != '':
            distance_float = float(register["totalTravelDistance"])
            register["totalTravelDistance"] = distance_float
            return
        else:
            register["totalTravelDistance"] = 0

    def __calculate_distance(self, start, end, client_id):
        coordinates_start = self.__airports_distances[client_id][start]
        coordinates_end = self.__airports_distances[client_id][end]
        return (geodesic(coordinates_start, coordinates_end)).miles

    def get_correct_dictionary(self, client_id):
        if len(self.__airports_distances[client_id]) > 0:
            return
        while True:
            alt_client_id, airport_dictionary = self.__pipe.recv()
            self.__airports_distances[alt_client_id] = airport_dictionary
            if alt_client_id != client_id:
                continue
            break
