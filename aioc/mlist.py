import time
from random import Random

from .state import Node, NodeStatus, NodeMeta


class MList:

    def __init__(self, config, seed=None):
        self._config = config
        host, port = self._config.host, self._config.port
        self._address = (host, port)

        node = Node(host, port)
        meta = NodeMeta(
            node=node,
            incarnation=1,
            meta=b'',
            status=NodeStatus.ALIVE,
            state_change=time.time(),
            is_local=True)

        self._members = {node: meta}
        self._nodes = [node]

        self._dead = {}
        self._suspected = {}

        self._local_node = node
        self._random = Random(seed)

    @property
    def config(self):
        return self._config

    @property
    def num_nodes(self):
        return len(self._members)

    @property
    def local_node(self):
        return self._local_node

    @property
    def local_node_meta(self):
        return self._members[self._local_node]

    @property
    def nodes(self):
        return self._nodes

    def kselect(self, k: int, filter_func=None):
        node_metas = list(filter(filter_func, self._members.values()))

        if len(node_metas) < k:
            return node_metas

        selected_nodes = self._random.sample(node_metas, k)
        return selected_nodes

    def node_meta(self, node):
        # TODO: make immutable
        return self._members.get(node)

    def update_node(self, node_meta):
        self._members[node_meta.node] = node_meta
        self._nodes.append(node_meta.node)

    def select_gossip_nodes(self):
        def filter_func(node_meta):
            if node_meta.is_local:
                return False

            gossip_to_dead = self.config.gossip_to_dead
            if (node_meta.status == NodeStatus.DEAD and
                    (time.time() - node_meta.state_change) > gossip_to_dead):
                return False
            return True

        gossip_nodes = min(self.config.gossip_nodes, self.num_nodes - 1)
        return self.kselect(gossip_nodes, filter_func)
