import asyncio  # noqa

from .state import Ping, Suspect, NodeStatus, AckResp
from .utils import LClock


class FailureDetector:

    def __init__(self, mlist, udp_server, gossiper, lclock, loop):
        self._mlist = mlist
        self._udp_server = udp_server
        self._gossiper = gossiper
        self._probes = {}
        self._loop = loop
        self._node_timers = {}
        self._lclock = lclock

    async def probe(self) -> None:

        def filter_func(node_meta):
            if node_meta.node == self._mlist.local_node:
                return False
            # if node.status != ALIVE:
            #    return False
            return True

        nodes = self._mlist.kselect(1, filter_func)
        print("PROBE", nodes)
        for node in nodes:
            await self.ping_node(node)

    async def ping_node(self, node_meta) -> None:
        sequence_num = self._lclock.next_sequence_num()
        msg = Ping(self._mlist.local_node, sequence_num, node_meta.node)

        msgs = [msg]
        if node_meta.status != NodeStatus.ALIVE:
            s = Suspect(self._mlist.local_node, node_meta.node, 1)
            msgs.append(s)
        waiter = self._loop.create_future()
        self._probes[(node_meta.node, sequence_num)] = waiter
        self._udp_server.send_message(node_meta.node, *msgs)
        try:
            ack = await asyncio.wait_for(
                waiter, self._mlist.config.probe_timeout)
            assert(ack)
        except asyncio.TimeoutError as e:
            print('ping_node', 'TimeoutError', e)
            msg = Suspect(self._mlist.local_node,
                          node_meta.node,
                          node_meta.incarnation)
            self._gossiper.suspect(msg)

    def on_ping(self, message: Ping) -> None:
        sequence_num = message.sequence_num
        sender = message.sender
        ack = AckResp(self._mlist.local_node, sequence_num, b'ping')
        self._udp_server.send_message(sender, ack)

    def on_ping_request(self, message, addr):
        pass

    def on_ack(self, message):
        node = message.sender

        waiter = self._probes.pop((node, message.sequence_num), None)
        if waiter is not None and not waiter.cancelled():
            waiter.set_result(message)

    def on_nack(self, message, addr):
        pass
