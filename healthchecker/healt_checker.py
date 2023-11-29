from multiprocessing import Process
from multiprocessing import Queue
import socket
import logging
from client_socket import ClientSocket
from server_utils import read_exact, send_exact
PORT = 12345

ELECTION_MESSAGE = b'e'
ANSWER_MESSAGE = b'a'
COORDINATOR_MESSAGE = b'c'
HEART_BEAT = b'h'


class HealthCkecker:
    def __init__(self, id, total_amount, name):
        self.__id = id
        self.__total_amount = total_amount
        self.__name = name
        # name = health_ckecker_1
        self.__minors = {i: (f"{name[:-1]}{i}", PORT) for i in range(1, id)}
        print(f"Minors is: {self.__minors}")
        self.__majors = {i: (f"{name[:-1]}{i}",
                             PORT) for i in range(id+1, total_amount+1)}
        print(f"Majors is: {self.__majors}")
        self._minor_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._minor_socket.bind(('', PORT))
        self._minor_socket.listen(len(self.__minors))
        self.__minor_sockets = {}
        self.__major_sockets = {}
        self.__queue = Queue()
        self.__is_leader = False
        self.__leader_id = 0

    def start(self):
        print("Started creating minor list")
        for i in range(len(self.__minors.items())):
            print(f"Loop, i: {i}")
            self.__minor_sockets[i] = self.__accept_new_connection()
            print(f"socket: {self.__minor_sockets[i]}")
        for id, info in self.__majors.items():
            print(f"id: {id}, info: {info}")
            self.__major_sockets[id] = ClientSocket(info)
            self.__major_sockets[id]._start_connection()
        self.__start_leader_election()
        # start loop
        # un proceso por vecino, cuanto mas arriba mas pasivo
        # para cada mensaje que se envia(de forma activa) se espera una respuesta
        # si el leader no contesta, empezar leader election
    
    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """

        # Connection arrived
        logging.info('action: accept_connections | result: in_progress')
        c, addr = self._minor_socket.accept()
        logging.info(
            f'action: accept_connections | result: success | ip: {addr[0]}')
        return c

    def __start_leader_election(self):
        major_processes = []
        minor_processes = []
        for id, skt in self.__major_sockets.items():
            major_processes.append(Process(target=self.send_election_msg,
                                           args=(skt,)))
        for id, skt in self.__minor_sockets.items():
            major_processes.append(Process(target=self.receive_answer,
                                           args=(skt,)))
        # chequear en la cola si sos el leader
        #enviar coordinator message / actualizar estado
        # joinear procesos
        # salir


        # para cada major
        # levantar proceso con el socket asociado, enviar mensaje via sockt ELECTION_MESSAGE y espera rta con timeout
        # esperar n resultados en un pipe
        # joinear procesos

    def send_election_msg(self, skt):
        skt._send_exact(ELECTION_MESSAGE)
        answer = skt._read_exact(1)
        if answer == ANSWER_MESSAGE:
            self.__queue.put(answer)
        else:
            self.__queue.put(b'0')

    def receive_answer(self, skt):
        read_exact(skt, 1)
        send_exact(skt, ANSWER_MESSAGE)
