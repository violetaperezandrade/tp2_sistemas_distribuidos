from util.queue_methods import connect_mom, listen_on, acknowledge
import json


def callback(channel, method, properties, body):
    result = json.loads(body)
    if result.get("op_code") == 0:
        # EOF
        print("Received EOF")
        acknowledge(channel, method)
        return
    print("Got result for query " + str(result["queryNumber"]))
    result.pop('queryNumber', None)
    print(result)
    acknowledge(channel, method)


connection = connect_mom()
listen_on(connection.channel(), "output", callback)
