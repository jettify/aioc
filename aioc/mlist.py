import time

from random import Random
from .state import Node, ALIVE
from .utils import LClock


class MList:

    def __init__(self, config, seed=None):
        self._lclock = LClock()
        self._config = config

        host, port = self._config.host, self._config.port
        self._address = f'{host}:{port}'
        incarnation = self._lclock.incarnation
        node = Node('', self._address, incarnation, b'', ALIVE,
                    time.time())

        self._members = {node.address: node}
        self._nodes = [n for n in self._members.values()]

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
    def local_address(self):
        return self._address

    @property
    def local_node(self):
        return self._members[self._address]

    @property
    def nodes(self):
        return self._nodes

    def kselect(self, k: int, filter_func):
        nodes_map = {}
        n = k
        for i in range(3*k):
            selected_nodes = self._random.sample(self.nodes, n)
            nodes = list(filter(filter_func, selected_nodes))
            for n in nodes:
                if n.address not in nodes_map:
                    nodes_map[n.address] = n
            if len(nodes) >= k:
                break
            else:
                n -= len(nodes)

        return nodes_map.values()

    def get_node(self, address):
        # TODO: make immutable
        return self._members.get(address)

    def update_node(self, node):
        self._members[node.address] = node
        # TODO: append to random position
        self._nodes.append(node)
