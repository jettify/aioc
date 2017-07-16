import time

from random import Random
from .state import Node, ALIVE, NodeMeta, DEAD
from .utils import LClock


class MList:

    def __init__(self, config, seed=None):
        self._lclock = LClock()
        self._config = config

        host, port = self._config.host, self._config.port
        self._address = (host, port)
        incarnation = self._lclock.incarnation
        node = Node(host, port)
        meta = NodeMeta(node, incarnation, b'', ALIVE, time.time())

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
    def lclock(self):
        return self._lclock

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

    def kselect(self, k: int, filter_func):
        nodes_map = {}
        n = k
        for i in range(3*k):
            selected_nodes = self._random.sample(
                list(self._members.values()), n)
            node_metas = list(filter(filter_func, selected_nodes))
            for n in node_metas:
                if n.node not in nodes_map:
                    nodes_map[n.node] = n
            if len(node_metas) >= k:
                break
            else:
                n -= len(node_metas)

        return nodes_map.values()

    def node_meta(self, node):
        # TODO: make immutable
        return self._members.get(node)

    def update_node(self, node_meta):
        self._members[node_meta.node] = node_meta
        self._nodes.append(node_meta.node)

    def select_gossip_nodes(self):
        def filter_func(node_meta):
            if node_meta.node == self.local_node:
                return False

            gossip_to_dead = self.config.gossip_to_dead
            if (node_meta.status == DEAD and
                    (time.time() - node_meta.state_change) > gossip_to_dead):
                return False
            return True

        gossip_nodes = min(self.config.gossip_nodes, self.num_nodes - 1)
        return self.kselect(gossip_nodes, filter_func)
