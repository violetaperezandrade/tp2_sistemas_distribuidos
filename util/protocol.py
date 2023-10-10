import json


def encode_register(flight, opcode):
    flight["op_code"] = opcode
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


def encode_eof(opcode):
    eof = {"op_code": opcode, "remaining_nodes": 3}
    return json.dumps(eof)


def encode_eof_client(opcode):
    eof = {"op_code": opcode}
    eof = json.dumps(eof)
    eof_bytes = eof.encode('utf-8')
    eof_length_bytes = len(eof_bytes).to_bytes(2, byteorder="big")

    message = eof_length_bytes + eof_bytes
    return message
