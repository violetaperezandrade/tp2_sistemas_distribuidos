import json


def save_to_file(flight_list, filename):
    with open(filename, "a") as file:
        for flight in flight_list:
            file.write(json.dumps(flight) + '\n')
            file.flush()


def log_to_file(filename, line):
    with open(filename, "a") as file:
        file.write(line + '\n')
        file.flush()


def log_batch_to_file(filename, batch):
    with open(filename, "a") as file:
        for element in batch:
            file.write(f'{element}\n')
            file.flush()
