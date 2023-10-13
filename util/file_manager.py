def save_to_file(flight_list, filename):
    with open(filename, "a") as file:
        for flight in flight_list:
            file.write(str(flight) + '\n')
