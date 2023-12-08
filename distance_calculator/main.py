import multiprocessing
import os
import pika

from distance_calculator import DistanceCalculator
from dictionary_creator import DictionaryCreator
from util.launch_heartbeat_sender import launch_heartbeat_sender


def main():
    input_queue = os.getenv("INPUT_QUEUE", None)
    input_exchange = os.getenv("INPUT_EXCHANGE", None)
    output_queue = os.getenv("OUTPUT_QUEUE", None)
    name = os.getenv("HOSTNAME", None)
    node_id = int(os.environ['IDX'])
    ips = os.environ['IPS'].split(",")
    port = int(os.environ['PORT'])
    frequency = int(os.environ['FREQUENCY'])

    process_h = multiprocessing.Process(target=launch_heartbeat_sender,
                                        args=(node_id,
                                              ips,
                                              port,
                                              frequency))
    process_h.start()

    conn1, conn2 = multiprocessing.Pipe()
    dictionary_creator = DictionaryCreator(input_exchange,
                                           name, conn1)
    process = multiprocessing.Process(target=dictionary_creator.run,
                                      args=())
    distance_calculator = DistanceCalculator(input_exchange,
                                             input_queue,
                                             output_queue,
                                             conn2, process)
    process.start()

    try:
        distance_calculator.run()
        process.join()
    except (pika.exceptions.ChannelWrongStateError,
            pika.exceptions.ConnectionClosedByBroker):
        pass


if __name__ == '__main__':
    main()
