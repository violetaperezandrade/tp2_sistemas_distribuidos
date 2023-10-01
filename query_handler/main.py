from util.queue_middleware import QueueMiddleware
import json

COLUMNS = 8
FIELD_LEN = 2
MAX_STOPOVERS = 3
COLUMN_NAME = "segmentsDepartureAirportCode"

def callback(channel, method, properties, body):
    if body.startswith(b'00'):
        pass
        # EOF
    flight = json.loads(body)
    # body --> (1 bytes de opcode, 2 bytes de longitud total, 2 bytes long columna 1, columna 1, 2 bytes long columna n, columna n)
    
    stopovers = flight[COLUMN_NAME].split("||")

    if len(stopovers) + 1 > MAX_STOPOVERS :
        print(body)
        channel.basic_publish(exchange='filter_by_three_stopovers',
                           routing_key='', body=filtered_byte_array)
        channel.basic_ack(delivery_tag=method.delivery_tag)


    
    

rabbitmq_mw = QueueMiddleware()
rabbitmq_mw._create_fanout_exchange("filter_by_three_stopovers")
rabbitmq_mw.subscribe_to("cleaned_flight_registers", callback)

