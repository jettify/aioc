import struct

from collections import namedtuple
from enum import Enum
from functools import partial
from typing import Any

import msgpack


class EventType(str, Enum):
    JOIN = 'JOIN'
    UPDATE = 'UPDATE'
    LEAVE = 'LEAVE'


MESSAGE_TYPE_SIZE = 1
LENGTH_SIZE = 4


Node = namedtuple(
    'Node', ['host', 'port'])

NodeMeta = namedtuple(
    'NodeMeta', ['node', 'incarnation', 'meta', 'status', 'state_change'])

ALIVE = 1
DEAD = 2
SUSPECT = 3


Ping = namedtuple(
    "Ping", ["sender", "sequence_num", "target"])

IndirectPingReq = namedtuple(
    "IndirectPingReq", ["sender", "sequence_num", "target", "nack"])

AckResp = namedtuple(
    "AckResp", ["sender", "sequence_num", "payload"])


NackResp = namedtuple(
    "NackResp", ["sender", "sequence_num"])

Suspect = namedtuple(
    "Suspect", ["sender", "node", "incarnation"])

# alive is broadcast when we know a node is alive.
# Overloaded for nodes joining
Alive = namedtuple(
    "Alive", ["sender", "node", "incarnation", "meta"])

PushPull = namedtuple(
    "PushPull", ["sender", "nodes", "join"])


UserMsg = namedtuple(
    "UserMsg", ["sender", "incarnation"])


# dead is broadcast when we confirm a node is dead
# Overloaded for nodes leaving
Dead = namedtuple("Dead", ["sender", "incarnation", "node", "from_node"])

Config = namedtuple("Config", ["node_name", "node_address"])


PING_MSG = 1
INDIRECT_PING_MSG = 2
ACK_RESP_MSG = 3
SUSPECT_MSG = 4
ALIVE_MSG = 5
DEAD_MSG = 6
PUSH_PULL_MSG = 7
COMPOUND_MSG = 8
USER_MSG = 9
COMPRESS_MSG = 10
ENCRYPT_MSG = 11
NACK_RESP_MSG = 12


unpackb = partial(msgpack.unpackb, encoding='utf-8')


def decode_message(raw_payload: bytes):
    message_type = raw_payload[0]
    raw_payload = raw_payload[1:]
    d = unpackb(raw_payload)
    node = Node(*d[0])
    d = d[1:]

    if message_type == PING_MSG:
        msg = Ping(node, d[0], Node(*d[1]))

    elif message_type == INDIRECT_PING_MSG:
        msg = IndirectPingReq(node, d[0], Node(*d[1]), d[2])

    elif message_type == NACK_RESP_MSG:
        msg = NackResp(node, *d)

    elif message_type == ACK_RESP_MSG:
        msg = AckResp(node, *d)

    elif message_type == ALIVE_MSG:
        msg = Alive(node, Node(*d[0]), *d[1:])

    elif message_type == SUSPECT_MSG:
        msg = Suspect(node, Node(*d[0]), *d[1:])

    elif message_type == DEAD_MSG:
        msg = IndirectPingReq(node, d[0], Node(*d[1]), Node(*d[2]))

    elif message_type == PUSH_PULL_MSG:
        msg = PushPull(
            node, [NodeMeta(Node(*i[0]), *i[1:]) for i in d[0]], d[1])
    else:
        print(raw_payload, message_type)
        raise RuntimeError("no such message type")

    return msg


def decode_messages(raw_payload: bytes):
    message_type = raw_payload[0]
    if message_type == COMPOUND_MSG:
        m = decode_compaund(raw_payload[1:])
    else:
        m = [decode_message(raw_payload)]
    return m


def encode_message(message: Any) -> bytes:
    raw_message = msgpack.packb(message)
    message_type = 0
    if isinstance(message, Ping):
        message_type = PING_MSG

    elif isinstance(message, IndirectPingReq):
        message_type = INDIRECT_PING_MSG

    elif isinstance(message, AckResp):
        message_type = ACK_RESP_MSG

    elif isinstance(message, NackResp):
        message_type = NACK_RESP_MSG

    # state gossip messages
    elif isinstance(message, Alive):
        message_type = ALIVE_MSG

    elif isinstance(message, Suspect):
        message_type = SUSPECT_MSG

    elif isinstance(message, Dead):
        message_type = DEAD_MSG

    elif isinstance(message, PushPull):
        message_type = PUSH_PULL_MSG

    else:
        import ipdb
        ipdb.set_trace()
        raise RuntimeError("Message type is unknown")

    m_size = len(raw_message)
    fmt = '>B{}s'.format(m_size)
    raw_payload = struct.pack(fmt, message_type, raw_message)
    return raw_payload


def add_msg_size(raw_payload: bytes) -> bytes:
    m_size = len(raw_payload)
    fmt = '>I{}s'.format(m_size)
    raw_payload = struct.pack(fmt, m_size, raw_payload)
    return raw_payload


def decode_msg_size(raw_payload: bytes) -> int:
    s = raw_payload[:4]
    m_size, *_ = struct.unpack('>I', s)
    return m_size


def encode_messages(*messages):
    return make_compaund(*[encode_message(m) for m in messages])


def make_compaund(*raw_payloads):
    raw = b''.join([add_msg_size(p) for p in raw_payloads])
    m_size = len(raw)
    fmt = '>B{}s'.format(m_size)
    raw_payload = struct.pack(fmt, COMPOUND_MSG, raw)
    return raw_payload


def decode_compaund(raw):
    messages = []
    while raw:
        m_size = decode_msg_size(raw)
        raw_msg = raw[4: 4 + m_size]
        raw = raw[4 + m_size:]
        messages.append(decode_message(raw_msg))
    return messages
