def read_exact(skt, bytes_to_read):
    bytes_read = skt.recv(bytes_to_read)
    while len(bytes_read) < bytes_to_read:
        new_bytes_read = skt.recv(
            bytes_to_read - len(bytes_read))
        bytes_read += new_bytes_read
    return bytes_read


def send_exact(skt, answer):
    bytes_sent = 0
    while bytes_sent < len(answer):
        chunk_size = skt.send(answer[bytes_sent:])
        bytes_sent += chunk_size
