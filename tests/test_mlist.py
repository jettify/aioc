from aioc.mlist import MList
from aioc.state import NodeStatus, Node, NodeMeta



def test_basic_mlist_ctor(config):
    mlist = MList(config, seed=1234)
    assert mlist.config == config

    assert mlist.num_nodes == 1



def test_mutiple_nodes(config):
    mlist = MList(config, seed=1234)

    for i in range(100):
        node_meta = NodeMeta(
            node=Node('127.0.0.1', 8080 + i),
            incarnation=i,
            meta=b'',
            status=NodeStatus.ALIVE,
            state_change=1506970524,
            is_local=False)
        mlist.update_node(node_meta)

    assert mlist.num_nodes == 101

    local = Node(config.host, config.port)
    assert mlist.local_node == local
    assert mlist.node_meta(local).node == local



def test_mutiple_nodes_kselect(config):
    mlist = MList(config, seed=1234)

    for i in range(100):
        node_meta = NodeMeta(
            node=Node('127.0.0.1', 8080 + i),
            incarnation=i,
            meta=b'',
            status=NodeStatus.ALIVE,
            state_change=1506970524,
            is_local=False)
        mlist.update_node(node_meta)

    assert mlist.num_nodes == 101
    node_metas = mlist.kselect(10)
    assert len(node_metas) == 10

    def filter_func(node_meta):
        return node_meta.node.port > 8090

    node_metas = mlist.kselect(3, filter_func)
    assert len(node_metas) == 3

    node_metas = mlist.select_gossip_nodes()
    assert len(node_metas) == config.gossip_nodes
