import pika
import os
from multiprocessing import Process

from column_cleaner import ColumnCleaner
from util.launch_heartbeat_sender import launch_heartbeat_sender


def main():
    input_queue = os.getenv("INPUT_QUEUE", None)
    input_exchange = os.getenv("INPUT_EXCHANGE", None)
    output_exchange = os.getenv("OUTPUT_EXCHANGE", None)
    output_queue = os.getenv("OUTPUT_QUEUE", None)
    required_columns_flights = \
        os.getenv("REQUIRED_COLUMNS_FLIGHTS", '').split(',')
    required_columns_airports = \
        os.getenv("REQUIRED_COLUMNS_AIRPORTS", '').split(',')
    routing_key = os.getenv("ROUTING_KEY", '')
    node_id = int(os.environ['IDX'])
    ips = os.environ['IPS'].split(",")
    port = int(os.environ['PORT'])
    frequency = int(os.environ['FREQUENCY'])
    cleaner = ColumnCleaner(output_queue, output_exchange, input_queue,
                            required_columns_flights,
                            required_columns_airports,
                            routing_key)
    process = Process(target=launch_heartbeat_sender,
                      args=(node_id,
                            ips,
                            port,
                            frequency))
    process.start()
    try:
        cleaner.run(input_exchange)
    except (pika.exceptions.ChannelWrongStateError,
            pika.exceptions.ConnectionClosedByBroker):
        pass


if __name__ == '__main__':
    main()
