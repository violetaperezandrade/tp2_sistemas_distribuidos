from util.queue_methods import connect_mom, subscribe_to, acknowledge
import json


def callback(channel, method, properties, body):
    result = json.loads(body)
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

subscribe_to(channel, "3_or_more_stop_overs", callback)

channel.start_consuming()
channel.close()
connection.close()
