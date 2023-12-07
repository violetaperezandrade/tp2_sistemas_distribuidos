from .heartbeat_sender import HeartbeatSender


def launch_heartbeat_sender(node_id, ips, port, frequency):
    heartbeat_sender = HeartbeatSender(node_id, ips, port, frequency)
    heartbeat_sender.start()
