import os
import pika
from filter_by_three_stopovers import FilterByThreeStopovers


def main():

    input_exchange = os.getenv("INPUT_EXCHANGE", None)
    output_exchange = os.getenv("OUTPUT_EXCHANGE", None)
    input_queue = os.getenv("INPUT_QUEUE", None)
    output_queue = os.getenv("OUTPUT_QUEUE", None)
    columns_to_filter = os.getenv("COLUMNS_TO_FILTER", '').split(',')
    max_stopovers = os.getenv("MAX_STOPOVERS", 3)
    name = os.getenv("HOSTNAME")
    filterByStopOvers = FilterByThreeStopovers(columns_to_filter,
                                               max_stopovers, output_queue,
                                               input_queue, output_exchange,
                                               name)
    try:
        filterByStopOvers.run(input_exchange)
    except pika.exceptions.ChannelWrongStateError:
        pass


if __name__ == '__main__':
    main()
