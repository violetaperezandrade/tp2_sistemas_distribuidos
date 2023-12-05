import multiprocessing
from multiprocessing import Process

import pika
from filter_by_average import FilterByAverage
import os

from final_avg_calculator import FinalAvgCalculator


def main():
    output_queue = os.getenv("OUTPUT_QUEUE", None)
    input_queue = os.getenv("INPUT_QUEUE", None)
    avg_exchange = os.getenv("INPUT_EXCHANGE", None)
    exchange_queue = os.getenv("EXCHANGE_QUEUE", None)
    node_id = int(os.getenv("ID", None))
    name = os.getenv("HOSTNAME", None)
    total_reducers = int(os.getenv("REDUCERS_AMOUNT", 3))
    conn1, conn2 = multiprocessing.Pipe()
    final_avg_calculator = FinalAvgCalculator(avg_exchange, exchange_queue, node_id, name, total_reducers, conn2)
    process = Process(target=final_avg_calculator.run,
                      args=())
    filter_by_average = FilterByAverage(output_queue, input_queue, node_id, name, total_reducers, conn1, process)
    process.start()

    try:
        filter_by_average.run()
        process.join()
    except (pika.exceptions.ChannelWrongStateError, pika.exceptions.ConnectionClosedByBroker):
        pass


if __name__ == '__main__':
    main()
