import os
from random import randint
from time import sleep
import shutil

from util.constants import EOF_SENT, ACCEPTED, EOF_FLIGHTS_FILE, ALL_CLIENTS

MESSAGE_ID = 0
CLIENT_ID = 1
FILTERING_RESULT = 2
SUM = 1
COUNT = 2


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


def get_updated_sum_and_count(filename, missing_flight_set):
    correct_last_line(filename)
    total_fare = 0
    count = 0
    with open(filename, 'r') as file:
        for line in file:
            if line.endswith("#\n"):
                continue
            try:
                message_id, fare = tuple(line.split(","))
            except ValueError as e:
                continue
            message_id = int(message_id)
            fare = float(fare)
            if message_id not in missing_flight_set:
                continue
            total_fare += fare
            count += 1
            missing_flight_set.remove(message_id)
    return total_fare, count


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
            os.remove(file_path)
        except FileNotFoundError:
            pass


def delete_node_data(main_path, client_id):
    if client_id == ALL_CLIENTS:
        delete_client_data(folder_path=main_path)
        return
    delete_client_data(file_path=get_flights_log_file(main_path, client_id))


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


def check_files_single_line(directory, filename):
    log_file = os.path.join(directory, filename)
    old_file = os.path.join(directory, 'old_' + filename)
    tmp_file = os.path.join(directory, 'temp_' + filename)
    if os.path.exists(tmp_file):
        if os.path.exists(old_file):
            os.rename(tmp_file, log_file)
            os.remove(old_file)
        else:
            if last_character_is_hash(tmp_file):
                os.rename(log_file, old_file)
                os.rename(tmp_file, log_file)
                os.remove(old_file)
            else:
                os.remove(tmp_file)
    else:
        os.remove(old_file) if os.path.exists(old_file) else None


def create_eof_flights_message_filters(accepted_flights, filter_id, client_id):
    register = dict()
    register["op_code"] = EOF_FLIGHTS_FILE
    register["messages_sent"] = int(accepted_flights)
    register["client_id"] = int(client_id)
    register["filter_id"] = int(filter_id)
    return register


def go_to_sleep():
    sleepytime = randint(10, 15)
    print(f"Going to sleep for {sleepytime}")
    sleep(sleepytime)


def last_character_is_hash(file_path):
    with open(file_path, 'rb') as file:
        file.seek(-1, os.SEEK_END)
        last_byte = file.read(1)
        return last_byte == b'#'
