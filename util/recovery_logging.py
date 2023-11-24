import json
import os
from random import randint
from time import sleep

from util.constants import BEGIN_EOF, EOF_SENT, ACCEPTED, FILTERED
from util.file_manager import log_to_file, log_batch_to_file


def propagate_eof_column_cleaner(middleware, method,
                                 body, filename,
                                 propagate_message):
    register = json.loads(body)
    op_code = register["op_code"]
    log_to_file(filename, f"{BEGIN_EOF},{register.get('message_id')},{register.get('client_id')}")
    middleware.manual_ack(method)
    propagate_message(body, op_code)
    log_to_file(filename, f"{EOF_SENT},{register.get('message_id')},"
                          f"{register.get('client_id')}")


def propagate_eof_standard(middleware, method,
                           body, filename,
                           output_queue):
    register = json.loads(body)
    log_to_file(filename, f"{BEGIN_EOF},{register.get('message_id')},{register.get('client_id')}")
    middleware.manual_ack(method)
    middleware.send_message(output_queue, body)
    log_to_file(filename, f"{EOF_SENT},{register.get('message_id')},"
                          f"{register.get('client_id')}")


def recover_state(filename, middleware,
                  output_queue=None,
                  propagate_message=None):
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
                    print("Recovered state, no need to send anything")
                    return
                msg = {"op_code": op_code,
                       "message_id": message_id,
                       "client_id": client_id}
                if propagate_message:
                    propagate_message(json.dumps(msg), op_code)
                else:
                    middleware.send_message(output_queue, json.dumps(msg))
                log_to_file(filename, f"{EOF_SENT},{message_id},{client_id}")
    else:
        return


def message_duplicated(filename, message_id):
    if os.path.exists(filename):
        with open(filename, 'r') as file:
            for line in file:
                line = line.strip("\n")
                if line == message_id:
                    return True
    return False


def log_message_batch(filename, message_list):
    log_batch_to_file(filename, message_list)
    message_list.clear()


MESSAGE_ID = 0
FILTERING_RESULT = 1


def log_get_missing_flights(filename, missing_flight_set, first_message, total_reducers, eof_message_id):
    for i in range(first_message, eof_message_id, total_reducers):
        missing_flight_set.add(i)
    max_id = 0
    accepted_flights = set()
    with open(filename, 'r') as file:
        for line in file:
            line = line.split(",")
            message_id = int(line[MESSAGE_ID])
            if max_id < message_id:
                max_id = message_id
            if int(line[FILTERING_RESULT]) == ACCEPTED:
                accepted_flights.add(message_id)
            if message_id in missing_flight_set:
                missing_flight_set.remove(message_id)
    return len(accepted_flights)
