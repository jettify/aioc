import asyncio
import contextlib
from functools import partial

from .failure_detector import FailureDetector
from .gossiper import Gossiper
from .mlist import MList
from .net import create_server
from .pusher import Pusher
from .state import Alive
from .tcp import create_tcp_server
from .utils import Ticker
from . import state


__all__ = ('Cluster',)


class Cluster:

    def __init__(self, config, loop=None):
        self.config = config
        self._mlist = MList(config)
        self._listener = EventListener(loop)

        self._tcp_handler = None
        self._udp_server = None
        self._tcp_server = None
        self._fd = None

        self._fd_ticker = None
        self._gossip_ticker = None

        self._closing = False
        self._started = False
        self._loop = loop or asyncio.get_event_loop()

    async def boot(self):
        h, p = self.config.host, self.config.port
        loop = self._loop

        udp_server = await create_server(
            host=h, port=p, mlist=self._mlist, loop=loop)

        self._gossiper = Gossiper(self._mlist, self._listener)

        self._pusher = Pusher(self._mlist, self._gossiper, loop)
        self._fd = FailureDetector(self._mlist, self._gossiper, loop)

        udp_server.set_handler(self.handle)

        self._udp_server = udp_server

        tcp_server, tcp_handler = await create_tcp_server(
            host=h, port=p, mlist=self._mlist,
            gossiper=self._gossiper, loop=loop)
        self._tcp_handler = tcp_handler
        self._tcp_server = tcp_server

        self._gossip_ticker = Ticker(
            partial(self._gossiper.gossip, self._udp_server),
            self.config.gossip_interval,
            loop=loop)
        self._gossip_ticker.start()

        self._fd_ticker = Ticker(
            self._fd.probe, self.config.probe_interval,
            loop=loop)
        # self._fd_ticker.start()

        self._pusher_ticker = Ticker(
            self._pusher.push_pull, self.config.push_pull_interval,
            loop=loop)

        self._started = True

    async def join(self, *hosts) -> int:
        success = await self._pusher.join(*hosts)
        return success

    async def leave(self):
        self._closing = True
        await self._fd_ticker.stop()
        await self._gossip_ticker.stop()
        await self._listener.stop()
        await self._pusher_ticker.stop()

        await self._udp_server.close()
        self._tcp_server.close()
        await self._tcp_server.wait_closed()

    async def update_node(self, metadata):
        assert len(metadata) < 500
        incarnation = self._mlist.lclock.next_incarnation()
        n = (self._mlist.local_node_meta
             ._replace(meta=metadata, incarnation=incarnation))
        self._mlist.update_node(n)
        a = Alive(n.node, n.node, n.incarnation, n.meta)
        waiter = self._loop.create_future()
        self._gossiper.queue.put(a, waiter)
        await waiter

    @property
    def local_node(self):
        return self._mlist.local_node

    @property
    def local_node_meta(self):
        n = self._mlist.local_node
        return self._mlist.node_meta(n)

    @property
    def listener(self):
        return self._listener

    @property
    def members(self):
        return self._mlist.nodes

    @property
    def num_meber(self):
        return len(self._mlist.nodes)

    def get_health_score(self) -> int:
        return 1

    def handle(self, raw_message, addr, protocol):
        if raw_message[0] == state.COMPOUND_MSG:
            messages = state.decode_messages(raw_message)
        else:
            messages = [state.decode_message(raw_message)]
        for m in messages:
            try:
                self.handle_message(addr, m, protocol)
            except Exception as e:
                print(raw_message, addr, protocol)
                raise e

    def handle_message(self, addr, message, udp_cm):
        if isinstance(message, state.Ping):
            self.handle_ping(message, addr, udp_cm)

        elif isinstance(message, state.IndirectPingReq):
            self.handle_indirect_ping(message, addr)

        elif isinstance(message, state.AckResp):
            self.handle_ack(message, addr, udp_cm)

        elif isinstance(message, state.NackResp):
            self.handle_nack(message, addr, udp_cm)

        elif isinstance(message, state.Alive):
            self.handle_alive(message, addr, udp_cm)

        else:
            raise RuntimeError("Can not handle message", message)

    # fd
    def handle_ping(self, message, addr, udp_cm):
        self._fd.on_ping(message, addr)

    def handle_indirect_ping(self, message, node, udp_cm):
        self._fd.on_indirect_ping(message)
        print(message, node)
        pass

    def handle_ack(self, message, node, udp_cm):
        self._fd.on_ack(message)
        print("ACK", message, node, udp_cm)

    def handle_nack(self, message, node, udp_cm):
        self._fd.on_ping(message, node)

    # gossip
    def handle_alive(self, message, node, udp_cm):
        self._gossiper.alive(message)

    def handle_suspect(self, message, node, udp_cm):
        self._gossiper.suspect(message)

    def handle_dead(self, message, node, udp_cm):
        self._gossiper.dead(message)

    # user
    def handle_user(self, message, node, udp_cm):
        print(message, node)


class EventListener:

    def __init__(self, loop):
        self._loop = loop
        self._hander = None
        self._queue = asyncio.Queue(loop=loop)
        self._mover_task = asyncio.ensure_future(
            self._mover(), loop=loop)

    def notify(self, event_type, node):
        if self._hander is None:
            return
        self._queue.put_nowait((event_type, node))

    def add_handler(self, handler):
        self._hander = handler

    async def _mover(self):
        event_type, node = await self._queue.get()
        if self._hander is not None:
            try:
                await self._hander(event_type, node)
            except Exception as e:
                print(e)

    async def stop(self):
        self._closing = True
        if self._mover_task is None:
            return

        with contextlib.suppress(asyncio.CancelledError):
            self._mover_task.cancel()
            await self._mover_task
            self._mover_task = None
