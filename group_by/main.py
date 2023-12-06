from multiprocessing import Process
import os
import pika
from group_by import GroupBy
from util.heartbeat_sender import HeartbeatSender


def main():
    field_group_by = os.environ['FIELD_GROUP_BY'].split(",")
    input_exchange = os.environ['INPUT_EXCHANGE']
    reducers_amount = int(os.environ['REDUCERS_AMOUNT'])
    queue_group_by = os.environ['QUEUE_GROUP_BY']
    queue_group_by_secondary = os.getenv('QUEUE_GROUP_BY_SECONDARY', None)
    input_queue = os.environ['INPUT_QUEUE']
    listening_queue = os.environ['LISTENING_QUEUE']
    name = os.getenv("HOSTNAME")
    requires_several_eof = (os.getenv("SEVERAL_EOF", "false") == "true")
    requires_q5_eof = (os.getenv("QUERY_5_EOF", "false") == "true")
    handle_flights_log = (os.getenv("HANDLE_FLIGHTS_LOG", "false") == "true")
    node_id = int(os.environ['IDX'])
    ips = os.environ['IPS'].split(",")
    port = int(os.environ['PORT'])
    frequency = int(os.environ['FREQUENCY'])
    group_by = GroupBy(field_group_by, input_exchange,
                       reducers_amount, queue_group_by, listening_queue,
                       input_queue, name, requires_several_eof,
                       handle_flights_log,
                       queue_group_by_secondary, requires_q5_eof)

    process = Process(target=launch_healthchecker,
                      args=(node_id,
                            ips,
                            port,
                            frequency))
    process.start()
    try:
        group_by.run()
    except (pika.exceptions.ChannelWrongStateError,
            pika.exceptions.ConnectionClosedByBroker):
        pass


def launch_healthchecker(node_id, ips, port, frequency):
    heartbeat_sender = HeartbeatSender(node_id, ips, port, frequency)
    heartbeat_sender.start()


if __name__ == '__main__':
    main()
