import json
import os

from column_cleaner import ColumnCleaner
from configparser import ConfigParser
import pika


def main():
    input_queue = os.getenv("INPUT_QUEUE", None)
    input_exchange = os.getenv("INPUT_EXCHANGE", None)
    output_exchange = os.getenv("OUTPUT_EXCHANGE", None)
    output_queue = os.getenv("OUTPUT_QUEUE", None)
    required_columns_flights = os.getenv("REQUIRED_COLUMNS_FLIGHTS", '').split(',')
    required_columns_airports = os.getenv("REQUIRED_COLUMNS_AIRPORTS", '').split(',')
    routing_key = os.getenv("ROUTING_KEY", '')
    cleaner = ColumnCleaner(output_queue, output_exchange, input_queue,
                            required_columns_flights,
                            required_columns_airports,
                            routing_key)
    try:
        cleaner.run(input_exchange)
    except (pika.exceptions.ChannelWrongStateError, pika.exceptions.ConnectionClosedByBroker):
        pass


if __name__ == '__main__':
    main()
