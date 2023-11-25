import json
import os
from random import randint
from time import sleep

from util.constants import BEGIN_EOF, EOF_SENT, ACCEPTED, FILTERED
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
