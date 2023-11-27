import json
import signal

from util.constants import EOF_AIRPORTS_FILE, BEGIN_EOF
from util.file_manager import log_to_file
from util.initialization import initialize_exchanges, initialize_queues
from util.queue_middleware import QueueMiddleware

NUMBER_CLIENTS = 3


class DictionaryCreator:
    def __init__(self, input_exchange, name, pipe):
        self.__middleware = QueueMiddleware()
        self.__airports_distances = {key: dict() for key in range(1, NUMBER_CLIENTS + 1)}
        self.__exchange_queue = name + "_airports_queue"
        self.__pipe = pipe
        self.__input_exchange = input_exchange
        self.airport_flights_log = "distance_calculator/" + name + "_airport_log.txt"
        self.airport_state_log = "distance_calculator/" + name + "_state_log.txt"

    def run(self):
        signal.signal(signal.SIGTERM, self.__middleware.handle_sigterm)
        initialize_exchanges([self.__input_exchange], self.__middleware)
        initialize_queues([self.__exchange_queue],
                          self.__middleware)
        # Reconstruir estado y enviar los diccionarios ya construidos en caso de ser necesario
        # Hay dos posiblidades: o el diccionario esta completo (y solo hay que enviarlo)
        # O esta incompleto (y hay que seguir escuchando de la cola para completarlo). Este
        # caso es medio trivial, en cualquier caso se sigue escuchando
        self.__middleware.subscribe(self.__input_exchange,
                                    self.__airport_callback,
                                    self.__exchange_queue)

    def __airport_callback(self, body, method):
        register = json.loads(body)
        client_id = register["client_id"]
        if register["op_code"] == EOF_AIRPORTS_FILE:
            required_length = register["message_id"] - 1
            log_to_file(self.airport_state_log, f"{BEGIN_EOF},{register.get('message_id')},"
                                                f"{register.get('client_id')}")
            # Aca se esta enviando el diccionario como esta al momento de recibir el eof
            # Pero podria estar incompleto, asi que en realidad habria que logear el eof primero
            # y verificar para cada mensaje posterior si se completa el diccionario
            self.send_if_complete(client_id, required_length)
            # log_to_file(self.log_file, f"{EOF_SENT},{register.get('message_id')},"
            #                            f"{register.get('client_id')}")
            self.__middleware.manual_ack(method)
            return
        self.__store_value(register)
        self.__middleware.manual_ack(method)

    def __store_value(self, register):
        airport_code = register["Airport Code"]
        latitude = register["Latitude"]
        longitude = register["Longitude"]
        client_id = register["client_id"]
        delimiter = ","
        # Habria que verificar si esta completo el self.__airport_distances[client_id]
        # al recibir este mensaje
        log_to_file(self.airport_flights_log, str(delimiter.join([airport_code, latitude, longitude])))
        airport_dictionary = self.__airports_distances[client_id]
        airport_dictionary[airport_code] = (latitude, longitude)

    def send_if_complete(self, client_id, required_length):
        complete_dictionary = (required_length == self.__airports_distances[client_id])
        if complete_dictionary:
            self.__pipe.send((client_id, self.__airports_distances[client_id]))

    # def recover_dictionaries_from_log(self):
    #     Habria que levantar los diccionarios completos e incompletos del log
    #     Mandar los completos tambien
    #     Despues seguir escuchando de la cola