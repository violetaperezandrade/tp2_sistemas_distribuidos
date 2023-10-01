from util.queue_middleware import QueueMiddleware
import json


def callback(channel, method, properties, body):
    if body.startswith(b'00'):
        pass
        # EOF
    result = json.loads(body)
    print("Got result for query " + str(result["queryNumber"]))
    result.pop('queryNumber', None)
    print(result)
    channel.basic_ack(delivery_tag=method.delivery_tag)


rabbitmq_mw = QueueMiddleware()
rabbitmq_mw.listen_on("output", callback)
