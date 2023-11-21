import json


def save_to_file(flight_list, filename):
    with open(filename, "a") as file:
        for flight in flight_list:
            file.write(json.dumps(flight) + '\n')


def log_to_file(filename, state):
    with open(filename, "a") as file:
        file.write(state + '\n')
