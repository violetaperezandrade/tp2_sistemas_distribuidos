import os
import pika
from filter_by_three_stopovers import FilterByThreeStopovers


def main():

    output_exchange = os.getenv("OUTPUT_EXCHANGE", None)
    reducers_amount = int(os.getenv("REDUCERS_AMOUNT", 3))
    output_queue = os.getenv("OUTPUT_QUEUE", None)
    columns_to_filter = os.getenv("COLUMNS_TO_FILTER", '').split(',')
    max_stopovers = os.getenv("MAX_STOPOVERS", 3)
    name = os.getenv("HOSTNAME")
    filterByStopOvers = FilterByThreeStopovers(columns_to_filter,
                                               max_stopovers, output_queue,
                                               output_exchange,
                                               name, 
                                               reducers_amount)
    try:
        filterByStopOvers.run()
    except pika.exceptions.ChannelWrongStateError:
        pass


if __name__ == '__main__':
    main()
