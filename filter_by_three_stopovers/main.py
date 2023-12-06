import os
import pika
from multiprocessing import Process

from filter_by_three_stopovers import FilterByThreeStopovers
from util.heartbeat_sender import HeartbeatSender


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
    process = Process(target=launch_healthchecker,
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


def launch_healthchecker(node_id, ips, port, frequency):
    heartbeat_sender = HeartbeatSender(node_id, ips, port, frequency)
    heartbeat_sender.start()


if __name__ == '__main__':
    main()
