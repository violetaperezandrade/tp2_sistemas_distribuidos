from avg_calculator import AvgCalculator
import os
import pika


def main():

    input_queue = os.getenv("INPUT_QUEUE", None)
    output_exchange = os.getenv("OUTPUT_EXCHANGE", None)
    node_id = int(os.getenv("ID", None))
    name = os.getenv("HOSTNAME", None)
    total_reducers = int(os.getenv("TOTAL_REDUCERS", 3))
    avg_calculator = AvgCalculator("totalFare", output_exchange, input_queue,
                                   node_id, name, total_reducers)
    try:
        avg_calculator.run()
    except pika.exceptions.ChannelWrongStateError:
        pass


if __name__ == '__main__':
    main()
