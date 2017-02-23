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
    'Node', ['node_id', 'address', 'incarnation', 'meta', 'status',
             'state_change'])

ALIVE = 1
DEAD = 2
SUSPECT = 3


Ping = namedtuple(
    "Ping", ["sequence_num", "address"])

IndirectPingReq = namedtuple(
    "IndirectPingReq", ["sequence_num", "target", "port", "node_id", "nack"])

AckResp = namedtuple(
    "AckResp", ["sequence_num", "payload"])


NackResp = namedtuple(
    "NackResp", ["sequence_num"])

Suspect = namedtuple(
    "Suspect", ["address", "incarnation", "from_node"])

# alive is broadcast when we know a node is alive.
# Overloaded for nodes joining
Alive = namedtuple(
    "Alive", ["node_id", "address", "incarnation",   "meta"])

PushPull = namedtuple(
    "PushPull", ["nodes", "join"])


UserMsg = namedtuple(
    "UserMsg", ["incarnation", "node_id"])


# dead is broadcast when we confirm a node is dead
# Overloaded for nodes leaving
Dead = namedtuple("Dead", ["incarnation", "node_id", "from_node"])

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

    if message_type == PING_MSG:
        d = unpackb(raw_payload)
        msg = Ping(*d)

    elif message_type == INDIRECT_PING_MSG:
        d = unpackb(raw_payload)
        msg = IndirectPingReq(*d)

    elif message_type == NACK_RESP_MSG:
        d = unpackb(raw_payload)
        msg = NackResp(*d)

    elif message_type == ACK_RESP_MSG:
        d = unpackb(raw_payload)
        msg = AckResp(*d)

    elif message_type == ALIVE_MSG:
        d = unpackb(raw_payload)
        msg = Alive(*d)

    elif message_type == SUSPECT_MSG:
        d = unpackb(raw_payload)
        msg = Suspect(*d)

    elif message_type == DEAD_MSG:
        d = unpackb(raw_payload)
        msg = Dead(*d)

    elif message_type == PUSH_PULL_MSG:
        d = unpackb(raw_payload)
        msg = PushPull([Node(*i) for i in d[0]], d[1])
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
        raise RuntimeError("Message type is uknown")

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
