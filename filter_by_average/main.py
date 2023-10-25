import pika
from filter_by_average import FilterByAverage
import os


def main():
    output_queue = os.getenv("OUTPUT_QUEUE", None)
    input_queue = os.getenv("INPUT_QUEUE", None)
    avg_exchange = os.getenv("INPUT_EXCHANGE_1", None)
    cleaner_column_exchange = os.getenv("INPUT_EXCHANGE_2", None)
    node_id = os.getenv("ID", None)

    filter_by_average = FilterByAverage(output_queue, input_queue,
                                        cleaner_column_exchange, node_id)

    try:
        filter_by_average.run(avg_exchange)
    except pika.exceptions.ChannelWrongStateError:
        pass


if __name__ == '__main__':
    main()
