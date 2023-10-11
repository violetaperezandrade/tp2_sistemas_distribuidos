from util.queue_middleware import QueueMiddleware


def initialize_queues(queue_list, middleware):
    for queue in queue_list:
        if queue is None:
            return
        if queue == '':
            return
        middleware.create_queue(queue)


def initialize_exchanges(exchange_list, middleware):
    for exchange in exchange_list:
        if exchange is None:
            return
        if exchange == '':
            return
        middleware.create_exchange(exchange)
