import multiprocessing
import os
from multiprocessing import Process

from distance_calculator import DistanceCalculator
import pika

from dictionary_creator import DictionaryCreator


def main():
    input_queue = os.getenv("INPUT_QUEUE", None)
    input_exchange = os.getenv("INPUT_EXCHANGE", None)
    output_exchange = os.getenv("OUTPUT_QUEUE", None)
    exchange_queue = os.getenv("EXCHANGE_QUEUE", None)
    name = os.getenv("HOSTNAME", None)
    conn1, conn2 = multiprocessing.Pipe()
    distance_calculator = DistanceCalculator(input_exchange,
                                             input_queue,
                                             output_exchange,
                                             conn2)
    dictionary_creator = DictionaryCreator(input_exchange,
                                           name, conn1)
    process = Process(target=dictionary_creator.run,
                      args=())
    process.start()
    # processes.append(new_process)
    try:
        distance_calculator.run(exchange_queue)
    except pika.exceptions.ChannelWrongStateError:
        pass


if __name__ == '__main__':
    main()
