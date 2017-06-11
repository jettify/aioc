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

Connection = namedtuple(
    "Connection", ["reader", "writer"])

Node = namedtuple(
    "Node", ["host", "port"])

Join = namedtuple(
    "Join", ["message_id", "sender"])

JoinReply = namedtuple(
    "Join", ["message_id", "sender", "last_disconnected"])

ForwardJoin = namedtuple(
    'ForwardJoin', ['message_id', 'sender', 'joiner', 'ttl'])


Config = namedtuple("Config", ["node_name", "node_address"])


JOIN_MSG = 1
JOIN_REPLY_MSG = 2
FORWARD_JOIN_MSG = 3


unpackb = partial(msgpack.unpackb, encoding='utf-8')


def decode_message(raw_payload: bytes):
    message_type = raw_payload[0]
    raw_payload = raw_payload[1:]

    if message_type == JOIN_MSG:
        d = unpackb(raw_payload)
        msg = Join(d[0], Node(*d[1]))

    elif message_type == JOIN_REPLY_MSG:
        d = unpackb(raw_payload)
        msg = JoinReply(d[0], Node(*d[1]), d[2])

    elif message_type == FORWARD_JOIN_MSG:
        d = unpackb(raw_payload)
        msg = ForwardJoin(d[0], Node(*d[1]),  Node(*d[2]), d[3])

    else:
        print(raw_payload, message_type)
        raise RuntimeError("No such message type")

    return msg


def encode_message(message: Any) -> bytes:
    raw_message = msgpack.packb(message)
    message_type = 0

    if isinstance(message, Join):
        message_type = JOIN_MSG

    elif isinstance(message, JoinReply):
        message_type = JOIN_REPLY_MSG

    elif isinstance(message, ForwardJoin):
        message_type = FORWARD_JOIN_MSG

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
