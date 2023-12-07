from multiprocessing import Process
import socket
import signal

from util.client_socket import ClientSocket
from constants import PORT, LEADER, HEARTBEAT, ELECTION, COORDINATOR
from process_utils import leader_validation
from server_utils import read_exact
from heartbeat_listener import HeartbeatListener
from util.heartbeat_sender import HeartbeatSender

INVALID_LEADER_ID = -1
PORT_HB = 5000


class HealthChecker:
    def __init__(self, id, total_amount, name, nodes_idxs,
                 nodes_timeout, frequency):
        self.__id = id
        self.__total_amount = total_amount
        self.__name = name
        self.__nodes_idxs = nodes_idxs
        self._nodes_timeout = nodes_timeout
        self._frequency = frequency
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
        self.terminate = False

    def start(self):
        signal.signal(signal.SIGTERM, self.handle_sigterm)
        self.execute_next_state()

    def execute_next_state(self):
        while True and not self.terminate:
            if self.current_operation == HEARTBEAT:
                try:
                    self._minor_socket.settimeout(10.0)
                    socket = self.__accept_new_connection()
                except TimeoutError:
                    self.current_operation = ELECTION
                    continue
                self.await_leader_heartbeats(socket)
            if self.current_operation == ELECTION:
                self.launch_election()
            if self.current_operation == LEADER:
                print("Soy el liderrr")
                self.act_as_leader()
                continue

    def await_leader_heartbeats(self, socket):
        if self.__leader_id != INVALID_LEADER_ID:
            print(f"El lider es {self.__leader_id}")
        hosts = [
            f"{self.__name[:-1]}{i}"
            for i in range(1, self.__total_amount + 1)
            if i != self.__id
            ]
        heartbeat_sender = HeartbeatSender(self.__id-1,
                                           hosts, PORT_HB,
                                           self._frequency)
        sender_process = Process(target=heartbeat_sender.start, args=())
        sender_process.start()
        while True:
            try:
                msg = list(read_exact(socket, 2))
                self.__leader_id = msg[1]
            except (TimeoutError, BrokenPipeError):
                print("Lanzando eleccion")
                self.current_operation = ELECTION
                break
            # Es un busy wait el resto de esta funcion,
            # no esta muy bueno la verdad.
            # Escucha por un ratito en un socket nuevo para ver
            # si tiene que cambiar de lider.
            self._minor_socket.settimeout(0.5)
            try:
                socket2 = self.__accept_new_connection()
            except TimeoutError:
                continue
            msg = list(read_exact(socket2, 2))
            if msg[1] > self.__leader_id or self.terminate:
                self.__is_leader = False
                self.__leader_id = msg[1]
                sender_process.terminate()
                sender_process.join()
                break

    def act_as_leader(self):
        for idx, value in enumerate(self.__minors.items()):
            process = Process(target=leader_validation, args=(value[0],
                                                              value[1],
                                                              self.__id))
            process.start()

        nodes_list = [i for i in range(0, self.__total_amount)]
        nodes_list.remove(int(self.__name[-1]) - 1)
        processes = []
        for i in nodes_list:
            heartbeat_listener = \
                HeartbeatListener(PORT_HB+i, i, self._nodes_timeout)
            print(f"Launching process{i}, port: {PORT_HB+i}, node_id: {i}")
            process = Process(target=heartbeat_listener.start, args=())
            process.start()
            processes.append(process)

        for idx in self.__nodes_idxs:
            idx = int(idx)
            heartbeat_listener = \
                HeartbeatListener(PORT_HB+idx, idx, self._nodes_timeout)
            process = Process(target=heartbeat_listener.start, args=())
            process.start()
            processes.append(process)

        while True:
            self._minor_socket.settimeout(0.5)
            # Tengo que escuchar a ver si viene alguien a sacarme cada tanto
            # (por ejemplo si soy el 1 o el 2 y se
            # levanta el 3)
            try:
                socket = self.__accept_new_connection()
            except TimeoutError:
                continue
            msg = list(read_exact(socket, 2))
            if msg[0] == COORDINATOR or self.terminate:
                self.__is_leader = False
                self.current_operation = HEARTBEAT
                self.__leader_id = msg[1]
                for p in processes:
                    p.terminate()
                    p.join()
                break

    def launch_election(self):
        self.__is_leader = True
        for id in self.__majors.items():
            if self.__leader_id == id:
                continue
            try:
                self._minor_socket.settimeout(3.0)
                socket = self.__accept_new_connection()
            except OSError:
                continue
            msg = list(read_exact(socket, 2))
            if msg[0] == COORDINATOR:
                self.__leader_id = msg[1]
                self.__is_leader = False
        if self.__is_leader:
            self.__leader_id = self.__id
        for id, addr in self.__minors.items():
            try:
                socket = ClientSocket(addr)
                socket._start_connection()
            except OSError:
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

    def handle_sigterm(self, signum, sigframe):
        self.terminate = True
