from util.queue_methods import connect_mom, subscribe_to, acknowledge, listen_on
import json


def callback(channel, method, properties, body):
    result = json.loads(body)
    print(result)
    if result.get("op_code") == 0:
        # EOF
        acknowledge(channel, method)
        return
    result.pop('queryNumber', None)
    result.pop('op_code', None)
    print(result)
    acknowledge(channel, method)


connection = connect_mom()
channel = connection.channel()

listen_on(channel, "output", callback)

channel.close()
connection.close()
