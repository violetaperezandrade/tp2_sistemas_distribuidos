from util.queue_methods import (connect_mom,
                                acknowledge, listen_on)
import json
import multiprocessing
QUERIES = 4


def callback(channel, method, properties, body):
    result = json.loads(body)
    if result.get("op_code") == 0:
        # EOF
        acknowledge(channel, method)
        return
    result.pop('op_code', None)
    print(result)
    acknowledge(channel, method)


def run(query_number):
    connection = connect_mom()
    channel = connection.channel()

    queue_name = f"output_{query_number}"
    listen_on(channel, queue_name, callback)

    channel.start_consuming()
    channel.close()
    connection.close()


processes = []
for i in range(1, QUERIES+1):
    process = multiprocessing.Process(target=run, args=(i, ))
    processes.append(process)
    process.start()

for process in processes:
    process.join()
