from aioc.state import (encode_message, decode_message, encode_messages,
                        decode_messages)
from aioc.state import (Ping, Suspect, Node,
                        IndirectPingReq, AckResp, NackResp, Alive,
                        Dead, PushPull, NodeMeta
                        )


def test_basic():
    messages = [
        Ping(Node("host", 9001), 1, Node("host", 9001)),
        IndirectPingReq(Node("host", 9001), 1, Node("host", 9001), True),
        AckResp(Node("host", 9001), 1, "data"),
        NackResp(Node("host", 9001), 1),
        Suspect(Node("host", 9001), Node("host", 9001), 1),
        Alive(Node("host", 9001), Node("host", 9001), 1, "data"),
        Dead(Node("host", 9001), 1, Node("host", 9001), Node("host", 9001)),
        PushPull(Node("host", 9001),
                 [NodeMeta(Node("host", 9001), 1, "data", "ALIVE", 0)],
                 True)
    ]

    for msg in messages:
        raw_msg = encode_message(msg)
        decoded_msg = decode_message(raw_msg)
        assert decoded_msg == msg


def test_compaund():
    ping = Ping(Node("host", 9001), 1, Node("host", 9001))
    ack = AckResp(Node("host", 9001), 1, "data")
    raw_msg = encode_messages(ping, ack)
    ms = decode_messages(raw_msg)
    assert [ping, ack] == ms
