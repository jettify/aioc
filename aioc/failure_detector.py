import asyncio

from .state import Ping, Suspect, ALIVE, AckResp
from .net import send_udp_messages


class FailureDetector:

    def __init__(self, mlist, udp_server, loop):
        self._mlist = mlist
        self._udp_server = udp_server
        self._probes = {}
        self._loop = loop
        self._node_timers = {}

    async def probe(self):

        def filter_func(node):
            if node.address == self._mlist.local_node.address:
                return False
            if node.status != ALIVE:
                return False
            return True

        nodes = self._mlist.kselect(1, filter_func)
        for node in nodes:
            await self.ping_node(node)

    async def ping_node(self, node):
        sequence_num = self._mlist.lclock.next_sequence_num()
        msg = Ping(sequence_num, self._mlist.local_node.address)
        msgs = [msg]
        if node.status != ALIVE:
            s = Suspect(node.address, 1, self._mlist.local_node.address)
            msgs.append(s)
        waiter = self._loop.create_future()
        self._probes[(node.address, sequence_num)] = waiter
        send_udp_messages(self._udp_server, node.address, *msgs)
        try:
            ack = await asyncio.wait_for(waiter, self._config.probe_timeout)
            print(ack)
        except asyncio.TimeoutError:
            raise

    def on_ping(self, message, addr):
        sequence_num = message.sequence_num
        ack = AckResp(sequence_num, b'ping')
        send_udp_messages(self._udp_server, addr, ack)

    def on_ping_request(self, message, addr):
        pass

    def on_ack(self, message, addr):
        waiter = self._probes.pop((addr, message.sequence_num), None)
        if waiter is not None and not waiter.cancelled():
            waiter.set_result(message)

    def on_nack(self, message, addr):
        pass
