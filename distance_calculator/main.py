import multiprocessing
import os
from multiprocessing import Process

from distance_calculator import DistanceCalculator
import pika

from dictionary_creator import DictionaryCreator


def main():
    input_queue = os.getenv("INPUT_QUEUE", None)
    input_exchange = os.getenv("INPUT_EXCHANGE", None)
    output_queue = os.getenv("OUTPUT_QUEUE", None)
    name = os.getenv("HOSTNAME", None)
    conn1, conn2 = multiprocessing.Pipe()
    dictionary_creator = DictionaryCreator(input_exchange,
                                           name, conn1)
    process = Process(target=dictionary_creator.run,
                      args=())
    distance_calculator = DistanceCalculator(input_exchange,
                                             input_queue,
                                             output_queue,
                                             conn2, process)
    process.start()
    try:
        distance_calculator.run()
        process.join()
    except (pika.exceptions.ChannelWrongStateError, pika.exceptions.ConnectionClosedByBroker):
        pass


if __name__ == '__main__':
    main()
