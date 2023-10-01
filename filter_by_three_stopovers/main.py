from util.queue_middleware import QueueMiddleware
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
    # body --> (1 bytes de opcode, 2 bytes de longitud total, 2 bytes long columna 1, columna 1, 2 bytes long columna n, columna n)
    
    stopovers = flight[COLUMN_NAME].split("||")[:-1]
    print(stopovers)
    if len(stopovers) >= MAX_STOPOVERS :
        message=create_message(flight, stopovers)
        print(message)
        channel.basic_publish(exchange='',
                           routing_key='output', body=message)
        channel.basic_ack(delivery_tag=method.delivery_tag)


def create_message( flight, stopovers ):
    message = dict()
    for i in range(len(COLUMNS_TO_FILTER)):
        message[COLUMNS_TO_FILTER[i]] = flight[COLUMNS_TO_FILTER[i]]
    
    message["stopovers"] = stopovers

    return message
    

rabbitmq_mw = QueueMiddleware()
rabbitmq_mw.create_queue("output")
rabbitmq_mw.subscribe_to("cleaned_flight_registers", callback)

