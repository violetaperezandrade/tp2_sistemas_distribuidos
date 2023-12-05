import json
import struct


def encode_register(flight, opcode):
    flight["op_code"] = opcode
    flight = json.dumps(flight)
    flight_bytes = flight.encode('utf-8')
    flight_length_bytes = len(flight_bytes).to_bytes(2, byteorder="big")

    message = flight_length_bytes + flight_bytes
    return message


def get_opcode(payload):
    return payload[0].get("op_code")


def decode_to_str(payload):
    flight_str = payload.decode('utf-8')
    return flight_str


def encode_eof(opcode, message_id, client_id):
    eof = {"op_code": opcode, "message_id": message_id, "client_id": client_id}
    return json.dumps(eof)


def encode_eof_client(opcode):
    eof = [{"op_code": opcode}]
    eof = json.dumps(eof)
    eof_bytes = eof.encode('utf-8')
    eof_length_bytes = len(eof_bytes).to_bytes(3, byteorder="big")

    message = eof_length_bytes + eof_bytes
    return message


def encode_sigterm_msg(opcode):
    eof = {"op_code": opcode}
    return json.dumps(eof)


def encode_registers_batch(batch, op_code, client_id):
    batch = list(map(lambda d: {**d, "op_code": op_code, "client_id": client_id}, batch))
    data_json = json.dumps(batch)
    payload_length = len(data_json)
    header = struct.pack('!I', payload_length & 0xFFFFFF)
    message = header[1:] + data_json.encode('utf-8')
    return message


def get_opcode_batch(payload):
    registers = json.loads(payload.decode('utf-8'))
    op_code = registers[0].get("op_code")
    return registers, op_code


def decode_server_ack(msg):
    return int.from_bytes(msg, byteorder='big')


def encode_server_ack(ack):
    return ack.to_bytes(1, byteorder='big')


def decode_query_result(payload):
    return json.loads(payload.decode('utf-8'))


def encode_query_result(result):
    result_bytes = (json.dumps(result)).encode('utf-8')
    result_length_bytes = len(result_bytes).to_bytes(2, byteorder="big")

    message = result_length_bytes + result_bytes
    return message


def encode_signal(code):
    result_bytes = code.to_bytes(1, byteorder="big")
    result_length_bytes = len(result_bytes).to_bytes(2, byteorder="big")

    message = result_length_bytes + result_bytes
    return message
