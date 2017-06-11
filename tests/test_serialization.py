from aioc.state import encode_message, decode_message, add_msg_size
from aioc.state import Join, ForwardJoin, Node


def test_basic_join():
    join = Join(1, Node('host', 9000))
    raw_msg = encode_message(join)
    net_msg = add_msg_size(raw_msg)

    decoded_join = decode_message(net_msg[4:])
    assert decoded_join == join


def test_forward_join():
    fjoin = ForwardJoin(1, Node('host', 9000), Node('host', 9001), 7)
    raw_msg = encode_message(fjoin)
    net_msg = add_msg_size(raw_msg)

    decoded_join = decode_message(net_msg[4:])
    assert decoded_join == fjoin
