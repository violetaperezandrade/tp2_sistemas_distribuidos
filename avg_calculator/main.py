import os
import pika
from multiprocessing import Process
from avg_calculator import AvgCalculator
from util.launch_heartbeat_sender import launch_heartbeat_sender


def main():

    input_queue = os.getenv("INPUT_QUEUE", None)
    output_exchange = os.getenv("OUTPUT_EXCHANGE", None)
    node_id = int(os.getenv("ID", None))
    name = os.getenv("HOSTNAME", None)
    total_reducers = int(os.getenv("TOTAL_REDUCERS", 3))
    node_id = int(os.environ['IDX'])
    ips = os.environ['IPS'].split(",")
    port = int(os.environ['PORT'])
    frequency = int(os.environ['FREQUENCY'])

    avg_calculator = AvgCalculator("totalFare", output_exchange, input_queue,
                                   node_id, name, total_reducers)

    process = Process(target=launch_heartbeat_sender,
                      args=(node_id,
                            ips,
                            port,
                            frequency))
    process.start()

    try:
        avg_calculator.run()
    except (pika.exceptions.ChannelWrongStateError,
            pika.exceptions.ConnectionClosedByBroker):
        pass


if __name__ == '__main__':
    main()
