from multiprocessing import Process
import socket

from client_socket import ClientSocket
from constants import PORT, LEADER, HEARTBEAT, ELECTION, COORDINATOR
from process_utils import leader_validation
from server_utils import read_exact

INVALID_LEADER_ID = -1
class HealthChecker:
    def __init__(self, id, total_amount, name):
        self.__id = id
        self.__total_amount = total_amount
        self.__name = name
        self.__minors = {i: (f"{name[:-1]}{i}",
                             PORT) for i in range(1, id)}
        self.__majors = {i: (f"{name[:-1]}{i}",
                             PORT) for i in range(id + 1, total_amount + 1)}
        self._minor_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._minor_socket.bind(('', PORT))
        self._minor_socket.listen(total_amount)
        self.__is_leader = False
        self.__leader_id = INVALID_LEADER_ID
        self.current_operation = HEARTBEAT
        if self.__id == self.__total_amount:
            self.current_operation = LEADER
            self.__is_leader = True

    def start(self):
        self.execute_next_state()

    def execute_next_state(self):
        while True:
            if self.current_operation == HEARTBEAT:
                try:
                    self._minor_socket.settimeout(10.0)
                    socket = self.__accept_new_connection()
                except TimeoutError as e:
                    self.current_operation = ELECTION
                    continue
                self.await_leader_heartbeats(socket)
            if self.current_operation == ELECTION:
                self.launch_election()
            if self.current_operation == LEADER:
                print("Soy el lider")
                self.act_as_leader()
                continue

    def await_leader_heartbeats(self, socket):
        if self.__leader_id != INVALID_LEADER_ID:
            print(f"El lider es {self.__leader_id}")
        while True:
            try:
                msg = list(read_exact(socket, 2))
                self.__leader_id = msg[1]
            except (TimeoutError, BrokenPipeError) as e:
                print(f"Lanzando eleccion")
                self.current_operation = ELECTION
                break
            # Es un busy wait el resto de esta funcion, no esta muy bueno la verdad.
            # Escucha por un ratito en un socket nuevo para ver si tiene que cambiar de lider.
            self._minor_socket.settimeout(0.5)
            try:
                socket2 = self.__accept_new_connection()
            except TimeoutError:
                continue
            msg = list(read_exact(socket2, 2))
            if msg[1] > self.__leader_id:
                self.__is_leader = False
                self.__leader_id = msg[1]
                break

    def act_as_leader(self):
        for idx, value in enumerate(self.__minors.items()):
            process = Process(target=leader_validation, args=(value[0], value[1], self.__id))
            process.start()
        while True:
            # Parte de la accion del lider seria levantar a los nodos caidos en alguna linea por aca
            self._minor_socket.settimeout(0.5)
            # Tengo que escuchar a ver si viene alguien a sacarme cada tanto (por ejemplo si soy el 1 o el 2 y se
            # levanta el 3)
            try:
                socket = self.__accept_new_connection()
            except TimeoutError:
                continue
            msg = list(read_exact(socket, 2))
            if msg[0] == COORDINATOR:
                self.__is_leader = False
                self.current_operation = HEARTBEAT
                self.__leader_id = msg[1]
                break


    def launch_election(self):
        self.__is_leader = True
        for id  in self.__majors.items():
            if self.__leader_id == id:
                continue
            try:
                self._minor_socket.settimeout(3.0)
                socket = self.__accept_new_connection()
            except OSError as e:
                continue
            msg = list(read_exact(socket,2))
            if msg[0] == COORDINATOR:
                self.__leader_id = msg[1]
                self.__is_leader = False
        if self.__is_leader:
            self.__leader_id = self.__id
        for id, addr in self.__minors.items():
            try:
                socket = ClientSocket(addr)
                socket._start_connection()
            except OSError as e:
                continue
            message = bytes([HEARTBEAT, self.__id])
            if self.__is_leader:
                message = bytes([COORDINATOR, self.__id])
            socket._send_exact(message)
            socket._close_connection()
        if self.__is_leader:
            self.current_operation = LEADER
        else:
            self.current_operation = HEARTBEAT

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """

        # Connection arrived
        c, addr = self._minor_socket.accept()

        return c
