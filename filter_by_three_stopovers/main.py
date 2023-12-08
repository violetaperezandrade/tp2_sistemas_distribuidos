import os
import pika
from multiprocessing import Process

from filter_by_three_stopovers import FilterByThreeStopovers
from util.launch_heartbeat_sender import launch_heartbeat_sender


def main():

    output_exchange = os.getenv("OUTPUT_EXCHANGE", None)
    reducers_amount = int(os.getenv("REDUCERS_AMOUNT", 3))
    output_queue = os.getenv("OUTPUT_QUEUE", None)
    columns_to_filter = os.getenv("COLUMNS_TO_FILTER", '').split(',')
    max_stopovers = os.getenv("MAX_STOPOVERS", 3)
    name = os.getenv("HOSTNAME")
    node_id = int(os.environ['IDX'])
    ips = os.environ['IPS'].split(",")
    port = int(os.environ['PORT'])
    frequency = int(os.environ['FREQUENCY'])
    filterByStopOvers = FilterByThreeStopovers(columns_to_filter,
                                               max_stopovers, output_queue,
                                               output_exchange,
                                               name,
                                               reducers_amount)
    process = Process(target=launch_heartbeat_sender,
                      args=(node_id,
                            ips,
                            port,
                            frequency))
    process.start()
    try:
        filterByStopOvers.run()
    except (pika.exceptions.ChannelWrongStateError,
            pika.exceptions.ConnectionClosedByBroker):
        pass


if __name__ == '__main__':
    main()
