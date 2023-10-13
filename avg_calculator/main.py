from avg_calculator import AvgCalculator
from configparser import ConfigParser
import os
import pika


def main():

    input_queue = os.getenv("INPUT_QUEUE", None)
    input_exchange = os.getenv("INPUT_EXCHANGE", None)
    output_exchange = os.getenv("OUTPUT_EXCHANGE", None)
    avg_calculator = AvgCalculator("totalFare", output_exchange,
                                   input_exchange, input_queue)
    try:
        avg_calculator.run()
    except pika.exceptions.ChannelWrongStateError:
        pass


if __name__ == '__main__':
    main()
