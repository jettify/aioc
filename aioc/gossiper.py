import time

from .state import (
    make_compaund,
    Alive, Suspect,
    add_msg_size, NodeMeta,
    EventType,
    NodeStatus)
from .dissemination_queue import DisseminationQueue


__all__ = ('Gossiper',)


MAX_UDP_PACKET_SIZE = 508


class Gossiper:

    def __init__(self, mlist, listener, lclock):
        self._mlist = mlist
        retransmit_mult = self._mlist.config.retransmit_mult
        self._queue = DisseminationQueue(self._mlist, retransmit_mult)
        self._listener = listener
        self._suspicions = {}
        self._lclock = lclock

    @property
    def queue(self):
        return self._queue

    async def gossip(self, udp_server):
        for node_meta in self._mlist.select_gossip_nodes():
            raw_payloads = self.queue.get_update_up_to(MAX_UDP_PACKET_SIZE)
            if not raw_payloads:
                return
            raw = add_msg_size(make_compaund(*raw_payloads))
            host, port = node_meta.node
            addr = (host, int(port))
            udp_server.send_raw_message(addr, raw)

    def alive(self, message, waiter=None):
        a = message
        node = a.node
        node_meta = self._mlist.node_meta(a.node)
        if node_meta is None:
            new_node_meta = NodeMeta(
                node,
                a.incarnation,
                a.meta,
                NodeStatus.ALIVE,
                time.time(),
                False)

            self._mlist.update_node(new_node_meta)
            self._listener.notify(EventType.JOIN, node)
        else:
            # TODO: check corner case here
            if a.incarnation <= node_meta.incarnation:
                return
            new_node_meta = node_meta._replace(
                incarnation=a.incarnation,
                meta=a.meta, status=NodeStatus.ALIVE,
                state_change=time.time())

            self._mlist.update_node(new_node_meta)
            self._suspicions.pop(a.node, None)

        self.queue.put(message, waiter=waiter)
        self._listener.notify(EventType.UPDATE, new_node_meta)

    def dead(self, message, waiter=None):
        node = message.node
        node_meta = self._mlist.node_meta(node)
        if node_meta is None:
            set_waiter(waiter)
            return

        if message.incarnation <= node_meta.incarnation:
            set_waiter(waiter)
            return

        is_local = message.node == self._mlist.local_node
        if node_meta.status == NodeStatus.DEAD and not is_local:
            set_waiter(waiter)
            return

        node_meta = node_meta._replace(
            status=NodeStatus.DEAD,
            incarnation=message.incarnation,
            state_change=time.time())

        self._mlist.update_node(node_meta)
        self.queue.put(message, waiter=waiter)
        self._listener.notify(EventType.LEAVE, node_meta.node)

    def suspect(self, message: Suspect, waiter=None):
        s = message
        node = self._mlist.node_meta(s.node)
        if node is None:
            set_waiter(waiter)
            return

        if s.incarnation < node.incarnation:
            set_waiter(waiter)
            return

        if s.node in self._suspicions:
            suspicion = self._suspicions[s.node]
            suspicion.confirm(s.sender)
            self.queue.put(message, waiter=waiter)
            return

        if s.node == self._mlist.local_node:
            self.refute(s)
            return

        suspicion = Suspicion(message.sender, )

        self._mlist.update_node(node)
        self.queue.put(message, waiter=waiter)
        self._listener.notify(EventType.UPDATE, node)

    def merge(self, message):
        for n in message.nodes:
            if n.status == NodeStatus.ALIVE:
                a = Alive(message.sender, n.node, n.incarnation, n.meta)
                self.alive(a)
            elif n.status in (NodeStatus.DEAD, NodeStatus.SUSPECT):
                # TODO: fix incorrect from_node address
                s = Suspect(message.sender, n.node, n.incarnation)
                self.suspect(s)

    def refute(self, msg):
        incarnation = self._lclock.next_incarnation()
        if msg.incarnation >= incarnation:
           incarnation = self._lclock.skip_incarnation(msg.incarnation)
        node_meta = self._mlist.local_node_meta
        a = Alive(node_meta.node, node_meta.node, incarnation, node_meta.meta)
        self.queue.put(a, waiter=None)


def set_waiter(fut):
    if (fut is not None) and (not fut.cancelled()):
        fut.set_result(True)
