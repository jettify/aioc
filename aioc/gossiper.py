import math
import time

from .state import (make_compaund, encode_message, Alive, Suspect,
                    add_msg_size, LENGTH_SIZE, NodeMeta,
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


    async def gossip(self, udp_server):
        for node_meta in self._mlist.select_gossip_nodes():
            bytes_available = 500
            raw_payloads = self.queue.get_update_up_to(bytes_available)
            if not raw_payloads:
                return
            raw = add_msg_size(make_compaund(*raw_payloads))
            host, port = node_meta.node
            addr = (host, int(port))
            print("GOSSIP", self._mlist.local_node, node_meta.node)
            await udp_server.send_raw_message(addr, raw)

    def alive(self, message):
        a = message
        node = a.node
        node_meta = self._mlist.node_meta(a.node)
        if node_meta is None:
            new_node_meta = NodeMeta(
                node, a.incarnation, a.meta, ALIVE, time.time())
            self._mlist.update_node(new_node_meta)
            self._listener.notify(EventType.JOIN, node)
        else:
            # TODO: check corner case here
            if a.incarnation <= node_meta.incarnation:
                return
            new_node_meta = node_meta._replace(
                incarnation=a.incarnation, meta=a.meta, status=ALIVE,
                state_change=time.time())

            self._mlist.update_node(new_node_meta)
            self._suspicions.pop(a.node, None)

        self.queue.put(message, waiter=None)
        self._listener.notify(EventType.UPDATE, new_node_meta)

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
                a = Alive(message.sender, n.node, n.incarnation, n.meta)
                self.alive(a)
            elif n.sate in (DEAD, SUSPECT):
                # TODO: fix incorrect from_node address
                s = Suspect(message.sender, n.node, n.incarnation)
                self.suspect(s)


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
        return a.node == b.node

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
