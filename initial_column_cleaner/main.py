from util.queue_middleware import QueueMiddleware

COLUMNS = 27


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
    for i in range(1, 28):
        column_len = int.from_bytes(
            body[bytes_readed:bytes_readed+2], byteorder="big")
        column_data = body[bytes_readed+2:bytes_readed+column_len]
        if i in indexes_needed:
            filtered_byte_array += body[bytes_readed:bytes_readed+2]
            filtered_byte_array += column_data
        bytes_readed += column_len + 2
    print(filtered_byte_array)
    channel.basic_publish(exchange='cleaned_flight_registers',
                          routing_key='', body=filtered_byte_array)


rabbitmq_mw = QueueMiddleware()
rabbitmq_mw._create_fanout_exchange("cleaned_flight_registers")
rabbitmq_mw.listen_on("full_flight_register", callback)
