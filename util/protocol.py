def encode(flight, opcode):
    byteArray = bytearray()
    byteArray += opcode.to_bytes(1, byteorder="big")
    byteTemp = bytearray()
    for column in flight:
        columnBytes = bytearray(column.encode())
        byteTemp += len(columnBytes).to_bytes(2, byteorder="big")
        byteTemp += columnBytes
    byteArray += len(byteTemp).to_bytes(2, byteorder="big")
    byteArray += byteTemp
    return byteArray

def decode(line):
    ...


