def handle_sigterm(queue_middleware):
    queue_middleware.finish()