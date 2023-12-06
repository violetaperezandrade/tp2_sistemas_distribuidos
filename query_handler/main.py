import multiprocessing
import os
import pika
import signal

from query_handler import QueryHandler
from util.launch_heartbeat_sender import launch_heartbeat_sender
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
    except (pika.exceptions.ChannelWrongStateError,
            pika.exceptions.ConnectionClosedByBroker):
        pass


def run_heartbeat():
    node_id = int(os.environ['IDX'])
    ips = os.environ['IPS'].split(",")
    port = int(os.environ['PORT'])
    frequency = int(os.environ['FREQUENCY'])
    process_hb = multiprocessing.Process(target=launch_heartbeat_sender,
                                         args=(node_id,
                                               ips,
                                               port,
                                               frequency))
    process_hb.start()


signal.signal(signal.SIGTERM, handle_sigterm)
run_heartbeat()
for i in range(1, QUERIES+1):
    process = multiprocessing.Process(target=run, args=(i,))
    processes.append(process)
    process.start()

for process in processes:
    process.join()
