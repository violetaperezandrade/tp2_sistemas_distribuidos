from util.queue_methods import (connect_mom,
                                send_message_to,
                                subscribe_to, acknowledge)
import json

COLUMNS = 8
FIELD_LEN = 2
MAX_STOPOVERS = 3
COLUMN_NAME = "segmentsArrivalAirportCode"
COLUMNS_TO_FILTER = ["legId", "startingAirport", "destinationAirport", "totalFare"]


def callback(channel, method, properties, body):
    if body.startswith(b'00'):
        pass
        # EOF
    flight = json.loads(body)
    stopovers = flight[COLUMN_NAME].split("||")[:-1]
    if len(stopovers) >= MAX_STOPOVERS:
        # Publish on query 3's queue here
        message = create_message(flight, stopovers, 1)
        send_message_to(channel, "output", json.dumps(message))
        acknowledge(channel, method)


def create_message(flight, stopovers, query_number):
    message = dict()
    for i in range(len(COLUMNS_TO_FILTER)):
        message[COLUMNS_TO_FILTER[i]] = flight[COLUMNS_TO_FILTER[i]]
    message["stopovers"] = stopovers
    message["queryNumber"] = query_number
    return message


connection = connect_mom()
subscribe_to(connection.channel(), "cleaned_flight_registers", callback)
