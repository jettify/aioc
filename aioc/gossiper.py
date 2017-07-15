import math
import time

from .state import (make_compaund, encode_message, Alive, Suspect,
                    add_msg_size, encode_messages, LENGTH_SIZE,
                    EventType, Node, ALIVE, SUSPECT, DEAD)


__all__ = ('Gossiper',)


class Gossiper:

    def __init__(self, mlist, listener):
        self._mlist = mlist
        self._queue = DisseminationQueue(self._mlist)
        self._listener = listener
        self._suspicions = {}

    @property
    def queue(self):
        return self._queue

    def select_gossip_nodes(self):
        def filter_func(node):
            if node.address == self._mlist.local_node.address:
                return False

            gossip_to_dead = self._mlist.config.gossip_to_dead
            if (node.status == DEAD and
                    (time.time() - node.state_change) > gossip_to_dead):
                return False
            return True

        gossip_nodes = min(self._mlist.config.gossip_nodes,
                           self._mlist.num_nodes - 1)
        return self._mlist.kselect(gossip_nodes, filter_func)

    async def gossip(self, udp_server):
        for node in self.select_gossip_nodes():
            bytes_available = 500
            raw_payloads = self.queue.get_update_up_to(bytes_available)
            if not raw_payloads:
                return
            raw = add_msg_size(make_compaund(*raw_payloads))
            host, port = node.address.split(":")
            addr = (host, int(port))
            print("gossip", raw, addr)
            udp_server.sendto(raw, addr)

    def alive(self, message):
        a = message
        node = self._mlist.get_node(a.address)
        if node is None:
            new_node = Node(a.node_id, a.address, a.incarnation, a.meta, ALIVE,
                            time.time())
            self._mlist.update_node(new_node)
            self._listener.notify(EventType.JOIN, new_node)
        else:
            # TODO: check corner case here
            if a.incarnation <= node.incarnation:
                return
            new_node = node._replace(incarnation=a.incarnation, meta=a.meta,
                                     status=ALIVE, state_change=time.time())
            self._mlist.update_node(new_node)
            self._suspicions.pop(a.address, None)

        self.queue.put(message, waiter=None)
        self._listener.notify(EventType.UPDATE, new_node)

    def dead(self, message):
        d = message
        node = self._mlist.get_node(d.addres)
        if node is None:
            return

        if d.incarnation <= node.incarnation:
            return

        is_local = d.address == self._mlist.local_node.address
        if node.status == DEAD and not is_local:
            return

        node = node._replace(status=DEAD, incarnation=d.incarnation,
                             state_change=time.time())
        self._mlist.update_node(node)
        self.queue.put(message, waiter=None)
        self._listener.notify(EventType.LEAVE, node)

    def suspect(self, message):
        s = message
        node = self._mlist.get_node(s.addres)
        if node is None:
            return

        if s.incarnation <= node.incarnation:
            return

        self._mlist.update_node(node)
        self.queue.put(message, waiter=None)
        self._events.notify(node)

    def merge(self, message):
        for n in message.nodes:
            if n.status == ALIVE:
                a = Alive(n.node_id, n.address, n.incarnation, n.meta)
                self.alive(a)
            elif n.sate in (DEAD, SUSPECT):
                # TODO: fix incorrect from_node address
                s = Suspect(n.incarnation, n.address, n.address)
                self.suspect(s)


def send_udp_messages(udp_server, address, *messages):
    raw = add_msg_size(encode_messages(*messages))
    host, port = address.split(":")
    addr = (host, int(port))
    udp_server.sendto(raw, addr)


class DisseminationQueue:

    def __init__(self, mlist):
        self._attempts_to_updates = {}
        self._hosts_to_attempt = {}
        self._mlist = mlist
        self._limit = 3
        self._queue = []

    def put(self, message, waiter=None):
        for i, (attempts, existing_msg, fut) in enumerate(self._queue):
            if self._invalidates(message, existing_msg):
                if (fut is not None) and (not fut.cancelled()):
                    fut.set_result(True)
                self._queue[i] = None
        self._queue = [q for q in self._queue if q is not None]
        attempts = 0
        self._queue.append((attempts, message, waiter))

    def _invalidates(self, a, b):
        return a.address == b.address

    def get_update_up_to(self, bytes_available):
        buffers = []
        factor = self._mlist.config.retransmit_mult

        num_nodes = self._mlist.num_nodes
        limit = retransmit_limit(factor, num_nodes)
        for i, (attempts, msg, fut) in enumerate(self._queue):
            raw_payload = encode_message(msg)
            if (len(raw_payload) + LENGTH_SIZE) <= bytes_available:
                buffers.append(raw_payload)
                bytes_available -= (len(raw_payload) + LENGTH_SIZE)

                if limit >= attempts:
                    self._queue[i] = None
                    if (fut is not None) and (not fut.cancelled()):
                        fut.set_result(True)
                else:
                    self._queue[i] = (attempts + 1, msg, fut)

        self._queue = sorted([q for q in self._queue if q is not None],
                             key=lambda i: i[0])
        return buffers


def retransmit_limit(retransmit_mult, num_nodes):
    node_scale = math.ceil(math.log(num_nodes + 1))
    return node_scale * retransmit_mult
