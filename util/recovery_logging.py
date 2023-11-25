import json
import os
from random import randint
from time import sleep

from util.constants import BEGIN_EOF, EOF_SENT, ACCEPTED, FILTERED, EOF_CLIENT
from util.file_manager import log_to_file, log_batch_to_file


MESSAGE_ID = 0
CLIENT_ID = 1
FILTERING_RESULT = 2


def recover_state(filename):
    if os.path.exists(filename):
        with open(filename, 'r') as file:
            try:
                last_line = file.readlines()[-1]
            except IndexError:
                return
            if last_line.endswith("\n"):
                last_line = last_line.strip('\n')
                op_code, message_id, client_id = tuple(last_line.split(','))
                op_code = int(op_code)
                if op_code == EOF_SENT:
                    return None
                msg = {"op_code": op_code,
                       "message_id": int(message_id),
                       "client_id": int(client_id)}
                return msg
    return None


def get_missing_flights(filename, missing_flight_set, first_message, total_reducers, eof_message_id, client_id):
    for i in range(first_message, eof_message_id, total_reducers):
        missing_flight_set.add(i)
    accepted_flights = set()
    with open(filename, 'r') as file:
        for line in file:
            line = line.split(",")
            message_id = int(line[MESSAGE_ID])
            flight_client_id = int(line[CLIENT_ID])
            if client_id != flight_client_id:
                continue
            if int(line[FILTERING_RESULT]) == ACCEPTED:
                accepted_flights.add(message_id)
            if message_id in missing_flight_set:
                missing_flight_set.remove(message_id)
    return len(accepted_flights)


def duplicated_message(filename, message_id, client_id):
    if os.path.exists(filename):
        with open(filename, 'r') as file:
            for line in file:
                line = line.strip("\n")
                if line[0] == message_id and line[1] == client_id:
                    return True
    return False


def go_to_sleep():
    sleepytime = randint(10, 20)
    print(f"Going to sleep for {sleepytime}")
    sleep(sleepytime)


def log_message_batch(filename, message_list):
    log_batch_to_file(filename, message_list)
    message_list.clear()

def handle_several_eofs(state_log_filename, 
                        flight, 
                        sender_id,
                        receivers_amount, 
                        necessary_lines):
    messages_sent = flight["messages_sent"]
    client_id = flight["client_id"]
    log_to_file(state_log_filename, f"{EOF_CLIENT},{sender_id},{messages_sent},{client_id}")
    verify_all_eofs_received(state_log_filename, client_id,receivers_amount, necessary_lines)

def verify_all_eofs_received(state_log_filename, client_id: str, reducers_amount, necessary_lines):
    eofs = set()
    with open(state_log_filename, "r") as file:
        for line in file:
            if not line.startswith(str(EOF_SENT)) and line.endswith("\n"):
                line = line.strip('\n')
                line = tuple(line.split(','))

                if int(line[3]) == int(client_id):
                    eofs.add((line[1], line[2]))
    if len(eofs) == reducers_amount:
        corrected_eof = 0
        for i in eofs:
            corrected_eof += int(i[1])
        necessary_lines[client_id] = corrected_eof

def send_eof_to_receivers(state_log_filename, queue_middleware, flight, necessary_lines, receivers_messages, receivers):
    client_id = flight["client_id"]
    if client_id in necessary_lines.keys():
        total = 0
        for value in receivers_messages[client_id]:
            total += value
        if total == necessary_lines[client_id]:
            flight["messages_sent"] = 0
            for i, reducer in enumerate(receivers):
                flight["messages_sent"] += receivers_messages[client_id][i]
            queue_middleware.send_message(reducer, json.dumps(flight))
            log_to_file(state_log_filename, f"{EOF_SENT},{client_id}")

def handle_receivers_message_per_client(flights_log_filename, flight, n_output_queue, receivers_messages, receivers_amount):
    message_id = flight.get("message_id")
    client_id = flight.get("client_id")
    if client_id not in receivers_messages.keys():
        receivers_messages[client_id] = [0] * receivers_amount
    receivers_messages[client_id][n_output_queue] += 1
    log_reducers_amounts = ""
    for i in range(receivers_amount):
        log_reducers_amounts = f",{receivers_messages[client_id][i]}"
    #TODO sacar este log y crear un log que pise y solo guarde la cantidad de mensajes recibidos
    log_to_file(flights_log_filename, f"{message_id},{client_id}{log_reducers_amounts}")
    # TODO en group agregar este metodo abajo de handle_receivers_message_per_client
    # self.send_eof_to_reducers(client_id, flight)