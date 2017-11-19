import pytest

from aioc.dissemination_queue import DisseminationQueue, retransmit_limit
from aioc.mlist import MList
from aioc.state import NodeStatus, Node, NodeMeta, Suspect


@pytest.fixture
def mlist(config):
    mlist = MList(config, seed=1234)
    for i in range(10):
        node_meta = NodeMeta(
            node=Node('127.0.0.1', 8080 + i),
            incarnation=1,
            meta=b'',
            status=NodeStatus.ALIVE,
            state_change=1506970524,
            is_local=False)
        mlist.update_node(node_meta)
    return mlist


def test_basic(mlist, config):
    retransmit_mult = config.retransmit_mult
    dq = DisseminationQueue(mlist, retransmit_mult)
    node1 = mlist.nodes[1]
    node2 = mlist.nodes[2]
    incarantion = 3

    msg1 = Suspect(mlist.local_node, node1, incarantion)
    msg2 = Suspect(mlist.local_node, node2, incarantion)
    dq.put(msg1)
    dq.put(msg2)
    buffers = dq.get_update_up_to(100)
    assert len(buffers) == 2


def test_retransmit_limit():
    v = retransmit_limit(3, 0)
    assert v == 0

    v = retransmit_limit(3, 1)
    assert v == 3

    v = retransmit_limit(3, 99)
    assert v == 6
