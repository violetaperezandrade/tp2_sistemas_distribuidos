import struct

OP_CODE_FLIGHT_REGISTER = 1
OP_CODE_EOF = 0


def encode_flight_register(flight):
    opcode_bytes = OP_CODE_FLIGHT_REGISTER.to_bytes(1, byteorder="big")
    flight_bytes = flight.encode('utf-8')
    flight_length_bytes = len(flight_bytes).to_bytes(2, byteorder="big")

    message = opcode_bytes + flight_length_bytes + flight_bytes
    return message


def encode_flight_register_q(flight):
    opcode_bytes = OP_CODE_FLIGHT_REGISTER.to_bytes(1, byteorder="big")
    flight_length_bytes = len(flight).to_bytes(2, byteorder="big")

    message = opcode_bytes + flight_length_bytes + flight
    return message


def encode_eof():
    opcode_bytes = OP_CODE_EOF.to_bytes(1, byteorder="big")
    payload_length = struct.pack('!H', 0)

    eof_message = opcode_bytes + payload_length
    return eof_message
