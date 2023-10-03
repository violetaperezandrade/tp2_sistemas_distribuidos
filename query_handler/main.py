from util.queue_methods import connect_mom, listen_on, acknowledge
import json


def callback(channel, method, properties, body):
    if body.startswith(b'00'):
        pass
        # EOF
    result = json.loads(body)
    print("Got result for query " + str(result["queryNumber"]))
    result.pop('queryNumber', None)
    print(result)
    acknowledge(channel, method)


connection = connect_mom()
listen_on(connection.channel(), "output", callback)
