from aioc.state import (encode_message, decode_message, encode_messages,
                        decode_messages)
from aioc.state import Ping, add_msg_size, Suspect


def test_basic():
    ping = Ping(1, 'host:9000')
    raw_msg = encode_message(ping)
    assert raw_msg == b'\x01\x92\x01\xa9host:9000'
    net_msg = add_msg_size(raw_msg)
    assert net_msg == b'\x00\x00\x00\r\x01\x92\x01\xa9host:9000'
    decoded_ping = decode_message(net_msg[4:])
    assert decoded_ping == ping


def test_compaund():
    ping = Ping(1, 'host:9000')
    suspect = Suspect(2, 'host:9000', 'host2:8000')
    raw_msg = encode_messages(ping, suspect)
    net_msg = add_msg_size(raw_msg)
    m = (b'\x00\x00\x00.\x08\x00\x00\x00\r\x01\x92\x01\xa9host:9000\x00\x00'
         b'\x00\x18\x04\x93\x02\xa9host:9000\xaahost2:8000')
    assert net_msg == m
    ms = decode_messages(net_msg[4:])
    assert [ping, suspect] == ms
