import os
import pika
import signal
from multiprocessing import Process

from reducer_group_by import ReducerGroupBy
from util.launch_heartbeat_sender import launch_heartbeat_sender

processes = []


def handle_sigterm(signum, sigframe):
    for process in processes:
        os.kill(process.pid, signal.SIGTERM)
        process.join()


signal.signal(signal.SIGTERM, handle_sigterm)


def main():
    field_group_by = os.environ['FIELD_GROUP_BY']
    output_queue = os.environ['OUTPUT_QUEUE']
    input_queue = os.environ['INPUT_QUEUE']
    query_number = int(os.environ['QUERY_NUMBER'])
    name = os.environ['HOSTNAME']
    node_id = int(os.environ['IDX'])
    ips = os.environ['IPS'].split(",")
    port = int(os.environ['PORT'])
    frequency = int(os.environ['FREQUENCY'])

    reducer_group_by = ReducerGroupBy(field_group_by, input_queue,
                                      output_queue, query_number,
                                      name)

    process = Process(target=launch_heartbeat_sender,
                      args=(node_id,
                            ips,
                            port,
                            frequency))
    process.start()
    processes.append(process)
    try:
        reducer_group_by.run()
    except pika.exceptions.ChannelWrongStateError:
        pass


if __name__ == '__main__':
    main()
