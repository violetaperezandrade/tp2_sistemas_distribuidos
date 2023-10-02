from util.queue_methods import connect_mom, listen_on, acknowledge, publish_on
import json

COLUMNS = 27
FIELD_LEN = 2
COLUMNS_NAME = ["legId", "startingAirport", "destinationAirport", "travelDuration",
                "totalFare", "totalTravelDistance",
                "segmentsArrivalAirportCode", "segmentsDepartureAirportCode"]


def callback(channel, method, properties, body):
    if body.startswith(b'00'):
        pass
        # EOF or SIGTERM
    indexes_needed = [1, 4, 5, 7, 13, 15, 20, 21]
    body = body[3:]
    bytes_read = 0
    filtered_columns = dict()
    j = 0
    for i in range(1, COLUMNS + 1):
        column_len = int.from_bytes(
            body[bytes_read:bytes_read + FIELD_LEN], byteorder="big")
        bytes_read += FIELD_LEN
        column_data = body[bytes_read:bytes_read + column_len]
        if i in indexes_needed:
            filtered_columns[COLUMNS_NAME[j]] = column_data.decode("utf-8")
            j += 1
        bytes_read += column_len
    message = json.dumps(filtered_columns)
    publish_on(channel, "cleaned_flight_registers", message)
    acknowledge(channel, method)


connection = connect_mom()
listen_on(connection.channel(),"full_flight_register", callback)
