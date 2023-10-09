import json

OP_CODE_FLIGHT_REGISTER = 1
OP_CODE_EOF = 0


def encode_flight_register(flight):
    flight["op_code"] = OP_CODE_FLIGHT_REGISTER
    flight = json.dumps(flight)
    flight_bytes = flight.encode('utf-8')
    flight_length_bytes = len(flight_bytes).to_bytes(2, byteorder="big")

    message = flight_length_bytes + flight_bytes
    return message


def get_opcode(payload):
    flight_str = payload.decode('utf-8')
    flight = json.loads(flight_str)
    return flight.get("op_code")


def decode_to_str(payload):
    flight_str = payload.decode('utf-8')
    return flight_str


def encode_eof():
    eof = {"op_code": 0, "remaining_nodes": 4}
    return json.dumps(eof)


def encode_eof_b():
    eof = {"op_code": 0}
    eof = json.dumps(eof)
    eof_bytes = eof.encode('utf-8')
    eof_length_bytes = len(eof_bytes).to_bytes(2, byteorder="big")

    message = eof_length_bytes + eof_bytes
    return message
