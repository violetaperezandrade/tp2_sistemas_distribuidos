import os
from random import randint
from time import sleep
import shutil

from util.constants import EOF_SENT, ACCEPTED, EOF_FLIGHTS_FILE

MESSAGE_ID = 0
CLIENT_ID = 1
FILTERING_RESULT = 2
SUM = 1
COUNT = 2


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


def get_missing_flights(filename, missing_flight_set, first_message,
                        total_reducers, eof_message_id, client_id):
    correct_last_line(filename)
    for i in range(first_message, eof_message_id, total_reducers):
        missing_flight_set.add(i)
    accepted_flights = set()
    with open(filename, 'r') as file:
        for line in file:
            if line.endswith("\n"):
                if line.endswith("#\n"):
                    continue
                else:
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


def get_missing_flights_for_avg_calculation(filename, missing_flight_set, first_message,
                                            total_reducers, eof_message_id):
    for i in range(first_message, eof_message_id, total_reducers):
        missing_flight_set.add(i)
    return get_updated_sum_and_count(filename, missing_flight_set)


def get_updated_sum_and_count(filename, missing_flight_set=None):
    correct_last_line(filename)
    with open(filename, 'r') as file:
        for line in file:
            if line.endswith("#\n"):
                continue
            line = line.split(",")
            message_id = int(line[MESSAGE_ID])
            sum = float(line[SUM])
            count = int(line[COUNT])
            if missing_flight_set is not None:
                if message_id in missing_flight_set:
                    missing_flight_set.remove(message_id)
    return sum, count


def correct_last_line(filename):
    with open(filename, 'a+') as file:
        line = file.read()
        if not line.endswith("\n") and line != "#":
            file.write('#\n')
            file.flush()


def get_flights_log_file(path, client_id):
    file = f"{path}/client_{client_id}_flights_log.txt"
    return file


def get_state_log_file(path):
    file = f"{path}/state_log.txt"
    return file


def delete_client_data(folder_path=None, file_path=None):
    if folder_path is not None:
        shutil.rmtree(folder_path, ignore_errors=True)
        return
    if file_path is not None:
        try:
            print(f"Deleting {file_path}")
            os.remove(file_path)
        except FileNotFoundError:
            pass


def recover_broken_line(lines, temp_file, old_file, log_file):
    with open(temp_file, 'w+') as file:
        file.writelines(lines)
    os.rename(log_file, old_file)
    os.remove(old_file)
    os.rename(temp_file, log_file)


def check_files(directory, log_file):
    all_files = os.listdir(directory)
    aux_files = [file for file in all_files if file.startswith("o") or file.startswith("t")]
    if len(aux_files) == 2:
        for file in aux_files:
            if file.startswith("o"):
                os.remove(file)
            else:
                os.rename(file, log_file)
    elif len(aux_files) == 1:
        if os.exists(log_file):
            os.remove(aux_files[0])
        else:
            os.rename(aux_files[0], log_file)
    return


def create_eof_flights_message_filters(accepted_flights, filter_id, client_id):
    register = dict()
    register["op_code"] = EOF_FLIGHTS_FILE
    register["messages_sent"] = int(accepted_flights)
    register["client_id"] = int(client_id)
    register["filter_id"] = int(filter_id)
    return register


def duplicated_message(filename, result_id):
    if os.path.exists(filename):
        with open(filename, 'r') as file:
            for line in file:
                line = line.strip("\n")
                if line == result_id:
                    return True
    return False


def create_if_necessary(path):
    os.makedirs(path, exist_ok=True)


def go_to_sleep():
    sleepytime = randint(10, 15)
    print(f"Going to sleep for {sleepytime}")
    sleep(sleepytime)
