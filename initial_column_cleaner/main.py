from util.queue_middleware import QueueMiddleware
import json

COLUMNS = 27
FIELD_LEN = 2
COLUMNS_NAME = ["legId", "startingAirport", "destinationAirport", "travelDuration", "totalFare", "totalTravelDistance", "segmentsArrivalAirportCode", "segmentsDepartureAirportCode"]


def callback(channel, method, properties, body):
    if body.startswith(b'00'):
        pass
        # EOF
    # body --> (1 bytes de opcode, 2 bytes de longitud total, 2 bytes long columna 1, columna 1, 2 bytes long columna n, columna n)
    filtered_byte_array = bytearray()
    indexes_needed = [1, 4, 5, 7, 13, 15, 20, 21]
    filtered_byte_array += body[:1]
    body = body[3:]
    bytes_readed = 0
    filtered_columns = dict()
    j = 0
    for i in range(1, COLUMNS + 1):
        column_len = int.from_bytes(
            body[bytes_readed:bytes_readed + FIELD_LEN], byteorder="big")
        bytes_readed += FIELD_LEN
        column_data = body[bytes_readed:bytes_readed + column_len]
        if i in indexes_needed:
            filtered_columns[COLUMNS_NAME[j]] =  column_data.decode("utf-8")
            j += 1
        bytes_readed += column_len
        # column_len = int.from_bytes(
        #     body[bytes_readed:bytes_readed + FIELD_LEN], byteorder="big")
        # column_data = body[bytes_readed + FIELD_LEN:bytes_readed+column_len]
        # if i in indexes_needed:
        #     filtered_byte_array += body[bytes_readed:bytes_readed + FIELD_LEN]
        #     filtered_byte_array += column_data
        # bytes_readed += column_len + FIELD_LEN
    message = json.dumps(filtered_columns)
    print(message)
    channel.basic_publish(exchange='cleaned_flight_registers',
                          routing_key='', body=message)
    channel.basic_ack(delivery_tag=method.delivery_tag)

rabbitmq_mw = QueueMiddleware()
rabbitmq_mw._create_fanout_exchange("cleaned_flight_registers")
rabbitmq_mw.listen_on("full_flight_register", callback)
