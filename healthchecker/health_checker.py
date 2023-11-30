import multiprocessing
from multiprocessing import Process
import socket
import logging
from time import sleep

from client_socket import ClientSocket
from server_utils import read_exact, send_exact

PORT = 12345

ELECTION_MESSAGE = b'e'
ANSWER_MESSAGE = b'a'
COORDINATOR_MESSAGE = b'c'
HEART_BEAT = b'h'

# Modos de operacion
REGULAR_OPERATING = 0
ELECTION_MODE = 1

# Pipe codes
OK = 0
CONNECTION_ERROR = 1
ELECTION_WON = 2


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
        self.__minor_sockets = {}
        self.__major_sockets = {}
        self.__is_leader = False
        self.__leader_id = 0
        self.election_necessary = True
        self.dropped_processes_ids = []

    def start(self):
        minor_processes = []
        minor_pipes = []
        major_processes = []
        major_pipes = []
        for id in range(len(self.__minors.items())):
            conn1, conn2 = multiprocessing.Pipe()
            process = Process(target=self.launch_minor_process, args=(id, conn2))
            minor_processes.append(process)
            process.start()
            minor_pipes.append(conn1)
        for id, addr in self.__majors.items():
            conn1, conn2 = multiprocessing.Pipe()
            process = Process(target=self.launch_major_process,
                              args=(id, addr, conn2))
            major_processes.append(process)
            process.start()
            major_pipes.append(conn1)
        new_pipes = []
        while True:
            # si se requiere eleccion la realizo
            if self.election_necessary:
                self.launch_election(major_pipes, minor_pipes)
                self.get_or_send_election_results(major_pipes, minor_pipes)
                self.election_necessary = False
                # esto habria que hacerlo unicamente cuando el nodo ya esta levantado
                # sino se va a freezear hasta que este listo
                major_pipes.extend(new_pipes)
                continue
            else:
                # sino opero normalmente
                # en este estado pueden que me avisen que alguien gano la eleccion
                for pipe in minor_pipes + major_pipes:
                    pipe.send(REGULAR_OPERATING)
                for pipe in minor_pipes:
                    # si hay un error aca hay que relanzar el proceso asi espera una conexion eventual
                    msg = pipe.recv()
                    # major_pipes.remove(pipe)
                leader_crashed = False
                new_pipes = []
                for pipe in major_pipes:
                    # si hay un error aca y es el lider hay que lanzar eleccion
                    # si no es el lider hay que seguir intentando conectarse asi cuando vuelve ya nos conectamos
                    msg = pipe.recv()
                    if msg[1] == CONNECTION_ERROR:
                        major_pipes.remove(pipe)
                        id = msg[0]
                        if self.__leader_id == id:
                            leader_crashed = True
                        conn1, conn2 = multiprocessing.Pipe()
                        process = Process(target=self.launch_major_process,
                                          args=(id, self.__majors[msg[0]], conn2))
                        self.dropped_processes_ids.append(id)
                        new_pipes.append(conn1)
                        major_processes.append(process)
                        process.start()
                    elif msg[1] == ELECTION_WON:
                        self.__is_leader = False
                        self.__leader_id = id
                        print(f"El lider es {self.__leader_id}")
                if leader_crashed:
                    self.election_necessary = True
                    continue

    def launch_election(self, major_pipes, minor_pipes):
        for pipe in major_pipes:
            pipe.send(ELECTION_MODE)
        for pipe in minor_pipes:
            pipe.send(REGULAR_OPERATING)
        self.__is_leader = True
        for pipe in major_pipes:
            message = pipe.recv()
            if message[1] == OK:
                self.__is_leader = False
        if self.__is_leader:
            print("Soy el lider")
        for pipe in minor_pipes:
            # se puede haber caido alguno, restartear en ese caso
            pipe.recv()

    def get_or_send_election_results(self, major_pipes, minor_pipes):
        for pipe in major_pipes:
            pipe.send(REGULAR_OPERATING)
        for pipe in minor_pipes:
            if self.__is_leader:
                pipe.send(ELECTION_WON)
            else:
                pipe.send(REGULAR_OPERATING)
        for pipe in major_pipes:
            message = pipe.recv()
            if message[1] == ELECTION_WON:
                self.__leader_id = message[0]
                print(f"El lider es {self.__leader_id}")

    # Se comunica con todos los nodos con mayor ID
    def launch_major_process(self, id, addr, pipe):
        # por ahi con dos loops?
        socket = ClientSocket(addr)
        socket.run()
        while True:
            msg = pipe.recv()
            try:
                if msg == REGULAR_OPERATING:
                    socket._send_exact(HEART_BEAT)
                elif msg == ELECTION_MODE:
                    socket._send_exact(ELECTION_MESSAGE)
                pipe_message = (id, OK)
                msg = socket._read_exact(1)
                if msg == COORDINATOR_MESSAGE:
                    pipe_message = (id, ELECTION_WON)
                pipe.send(pipe_message)
            except (ConnectionResetError, BrokenPipeError, TimeoutError):
                pipe_message = (id, CONNECTION_ERROR)
                pipe.send(pipe_message)
                break

    # Se comunica con todos los nodos con menor ID
    def launch_minor_process(self, id, pipe):
        # por ahi con dos loops?
        socket = self.__accept_new_connection()
        while True:
            msg = pipe.recv()
            try:
                socket_message = read_exact(socket, 1)
                pipe_message = (id, OK)
                if msg == REGULAR_OPERATING:
                    send_exact(socket, HEART_BEAT)
                if msg == ELECTION_WON:
                    send_exact(socket, COORDINATOR_MESSAGE)
                pipe.send(pipe_message)
            except (ConnectionResetError, BrokenPipeError, TimeoutError):
                pipe_message = (id, CONNECTION_ERROR)
                pipe.send(pipe_message)
                break

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """

        # Connection arrived
        c, addr = self._minor_socket.accept()

        return c
