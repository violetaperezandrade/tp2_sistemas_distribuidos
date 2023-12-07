import multiprocessing
import pika
import signal
import os

from multiprocessing import Process

from final_avg_calculator import FinalAvgCalculator
from filter_by_average import FilterByAverage
from util.launch_heartbeat_sender import launch_heartbeat_sender

processes = []


def handle_sigterm(signum, sigframe):
    for process in processes:
        os.kill(process.pid, signal.SIGTERM)
        process.join()


signal.signal(signal.SIGTERM, handle_sigterm)


def main():
    output_queue = os.getenv("OUTPUT_QUEUE", None)
    input_queue = os.getenv("INPUT_QUEUE", None)
    avg_exchange = os.getenv("INPUT_EXCHANGE", None)
    exchange_queue = os.getenv("EXCHANGE_QUEUE", None)
    node_id = int(os.getenv("ID", None))
    name = os.getenv("HOSTNAME", None)
    total_reducers = int(os.getenv("REDUCERS_AMOUNT", 3))
    node_idx = int(os.environ['IDX'])
    ips = os.environ['IPS'].split(",")
    port = int(os.environ['PORT'])
    frequency = int(os.environ['FREQUENCY'])

    process_hb = Process(target=launch_heartbeat_sender,
                         args=(node_idx,
                               ips,
                               port,
                               frequency))
    process_hb.start()
    processes.append(process_hb)

    conn1, conn2 = multiprocessing.Pipe()
    final_avg_calculator = FinalAvgCalculator(avg_exchange, exchange_queue,
                                              node_id, name, total_reducers,
                                              conn2)
    process = Process(target=final_avg_calculator.run,
                      args=())
    filter_by_average = FilterByAverage(output_queue, input_queue, node_id,
                                        name, total_reducers, conn1, process)
    process.start()
    processes.append(process)

    try:
        filter_by_average.run()
        process.join()
    except (pika.exceptions.ChannelWrongStateError,
            pika.exceptions.ConnectionClosedByBroker):
        pass


if __name__ == '__main__':
    main()
