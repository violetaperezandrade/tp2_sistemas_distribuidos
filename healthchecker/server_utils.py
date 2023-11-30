def read_exact(skt, bytes_to_read):
    bytes_read = skt.recv(bytes_to_read)
    while len(bytes_read) < bytes_to_read:
        new_bytes_read = skt.recv(
            bytes_to_read - len(bytes_read))
        if len(new_bytes_read) == 0:
            raise BrokenPipeError
        bytes_read += new_bytes_read
    return bytes_read


def send_exact(skt, answer):
    bytes_sent = 0
    while bytes_sent < len(answer):
        chunk_size = skt.send(answer[bytes_sent:])
        if chunk_size == 0:
            raise BrokenPipeError
        bytes_sent += chunk_size
