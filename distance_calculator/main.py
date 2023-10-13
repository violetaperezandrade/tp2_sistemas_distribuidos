import os

from distance_calculator import DistanceCalculator
import pika


def main():
    input_queue = os.getenv("INPUT_QUEUE", None)
    input_exchange = os.getenv("INPUT_EXCHANGE", None)
    output_exchange = os.getenv("OUTPUT_QUEUE", None)
    query_handler = DistanceCalculator(input_exchange,
                                       input_queue,
                                       output_exchange)
    try:
        query_handler.run()
    except pika.exceptions.ChannelWrongStateError:
        pass


if __name__ == '__main__':
    main()
