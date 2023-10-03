from util.queue_middleware import QueueMiddleware
import json

COLUMNS = 27
FIELD_LEN = 2
COLUMNS_NAME = ["legId", "startingAirport", "destinationAirport",
                "travelDuration",
                "totalFare", "totalTravelDistance",
                "segmentsArrivalAirportCode", "segmentsDepartureAirportCode"]


def callback(channel, method, properties, body):
    if body.startswith(b'00'):
        pass
        # EOF
    filtered_byte_array = bytearray()
    filtered_byte_array += body[:1]
    body = body[3:].decode('utf-8')
    body_dict = json.loads(body)
    filtered_columns = dict()
    for column in COLUMNS_NAME:
        filtered_columns[column] = body_dict[column]
    message = json.dumps(filtered_columns)
    channel.basic_publish(exchange='cleaned_flight_registers',
                          routing_key='', body=message)
    channel.basic_ack(delivery_tag=method.delivery_tag)


rabbitmq_mw = QueueMiddleware()
rabbitmq_mw._create_fanout_exchange("cleaned_flight_registers")
rabbitmq_mw.listen_on("full_flight_register", callback)
