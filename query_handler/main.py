import multiprocessing
import os
import pika
import signal

from query_handler import QueryHandler
QUERIES = 5
processes = []


def handle_sigterm(signum, sigframe):
    for process in processes:
        os.kill(process.pid, signal.SIGTERM)


def run(query_number):
    reducers = int(os.environ['TOTAL_REDUCERS'])
    if query_number in [1, 2]:
        reducers = 1
    query_handler = QueryHandler(query_number, reducers)
    try:
        query_handler.run()
    except pika.exceptions.ChannelWrongStateError:
        pass


signal.signal(signal.SIGTERM, handle_sigterm)
for i in range(1, QUERIES+1):
    process = multiprocessing.Process(target=run, args=(i,))
    processes.append(process)
    process.start()

for process in processes:
    process.join()
