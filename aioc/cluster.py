import asyncio
import contextlib

from .failure_detector import FailureDetector
from .gossiper import Gossiper
from .mlist import MList
from .net import create_server, MessageHandler
from .pusher import Pusher
from .state import Alive
from .tcp import create_tcp_server
from .utils import Ticker


__all__ = ('Cluster',)


class Cluster:

    def __init__(self, config, loop=None):
        self.config = config
        self._mlist = MList(config)
        self._listener = EventListener(loop)

        self._udp_handler = None
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

        udp_handler = MessageHandler(self._mlist, self._gossiper, self._fd,
                                     loop)
        udp_server.set_handler(udp_handler.handle)

        self._udp_handler = udp_handler
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

        self._udp_server.close()
        self._tcp_server.close()
        await self._udp_server.wait_closed()
        await self._tcp_server.wait_closed()

    async def update_node(self, metadata):
        assert len(metadata) < 500
        incarnation = self._mlist.lclock.next_incarnation()
        n = (self._mlist
             .local_node
             ._replace(meta=metadata, incarnation=incarnation))
        self._mlist.update_node(n)
        a = Alive(n.node_id, n.address, n.incarnation, n.meta)
        waiter = self._loop.create_future()
        self._gossiper.queue.put(a, waiter)
        await waiter

    @property
    def local_node(self):
        return self._mlist.local_node

    @property
    def listener(self):
        return self._listener

    @property
    def members(self):
        return self._mlist.nodes

    def get_node(self, address):
        return self._mlist.get_node(address)

    @property
    def num_meber(self):
        return len(self._mlist.nodes)

    def get_health_score(self) -> int:
        return 1


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
