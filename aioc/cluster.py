import asyncio
import contextlib
from functools import partial

from .failure_detector import FailureDetector
from .gossiper import Gossiper
from .mlist import MList
from .net import create_server
from .pusher import Pusher
from .state import Alive, PushPull, Dead
from .tcp import create_tcp_server
from .utils import Ticker
from . import state


__all__ = ('Cluster',)


class Cluster:

    def __init__(self, config, loop=None):
        self.config = config
        self._mlist = MList(config)
        self._listener = EventListener(loop)

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

        self._fd = FailureDetector(self._mlist, udp_server, self._gossiper, loop)

        udp_server.set_handler(self.handle)

        self._udp_server = udp_server

        tcp_server = await create_tcp_server(host=h, port=p, loop=loop)
        tcp_server.set_handler(self.handle_tcp_message)
        self._tcp_server = tcp_server

        self._pusher = Pusher(self._mlist, self._gossiper, tcp_server, loop)

        self._gossip_ticker = Ticker(
            partial(self._gossiper.gossip, self._udp_server),
            self.config.gossip_interval,
            loop=loop)
        self._gossip_ticker.start()

        self._fd_ticker = Ticker(
            self._fd.probe, self.config.probe_interval,
            loop=loop)
        self._fd_ticker.start()
        self._pusher_ticker = Ticker(
            self._pusher.push_pull, self.config.push_pull_interval,
            loop=loop)
        self._pusher_ticker.start()
        self._started = True

    async def join(self, *hosts) -> int:
        success = await self._pusher.join(*hosts)
        return success

    async def leave(self):
        incarnation = self._fd._lclock.next_incarnation()
        node = self._mlist.local_node
        msg = Dead(node, incarnation, node, node)
        self._closing = True
        await self._fd_ticker.stop()
        await self._gossip_ticker.stop()
        await self._listener.stop()
        await self._pusher_ticker.stop()

        await self._udp_server.close()
        await self._tcp_server.close()

    async def update_node(self, metadata):
        assert len(metadata) < 500
        incarnation = self._fd._lclock.next_incarnation()
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
        return len(self._mlist._members)

    def get_health_score(self) -> int:
        return 1

    async def handle_tcp_message(self, message, conn):
        if isinstance(message, state.Ping):
            await self.handle_tcp_ping(message, conn)

        elif isinstance(message, state.PushPull):
            await self.handle_push_pull(message, conn)

        elif isinstance(message, state.UserMsg):
            await self.handle_user(message, conn)
        else:
            await self.handle_unknown(message, conn)

    async def handle_tcp_ping(self, message, conn):
        sequence_num = message.sequence_num
        # TODO: handle ack payload correctly
        ack = state.AckResp(sequence_num, b'ping')
        raw = state.add_msg_size(state.encode_message(ack))
        conn.writer.write(raw)

    async def handle_user(self, message, conn):
        print(message, conn)

    async def handle_push_pull(self, message, conn):
        metas = list(self._mlist._members.values())
        resp = PushPull(self._mlist.local_node, metas, False)
        self._gossiper.merge(message)
        raw = state.add_msg_size(state.encode_message(resp))
        conn.writer.write(raw)

    async def handle_unknown(self, message, conn):
        print(message, conn)

    def handle(self, raw_message, addr, protocol):
        if raw_message[0] == state.COMPOUND_MSG:
            messages = state.decode_messages(raw_message)
        else:
            messages = [state.decode_message(raw_message)]
        for m in messages:
            try:
                self.handle_udp_message(m, protocol)
            except Exception as e:
                print(raw_message, addr, protocol)
                raise e

    def handle_udp_message(self, message, udp_cm):
        if isinstance(message, state.Ping):
            self.handle_ping(message)

        elif isinstance(message, state.IndirectPingReq):
            self.handle_indirect_ping(message, udp_cm)

        elif isinstance(message, state.AckResp):
            self.handle_ack(message, udp_cm)

        elif isinstance(message, state.NackResp):
            self.handle_nack(message, udp_cm)

        elif isinstance(message, state.Alive):
            self.handle_alive(message, udp_cm)

        elif isinstance(message, state.Dead):
            self.handle_dead(message, udp_cm)

        elif isinstance(message, state.Suspect):
            self.handle_suspect(message, udp_cm)

        else:
            raise RuntimeError("Can not handle message", message)

    # fd
    def handle_ping(self, message):
        self._fd.on_ping(message)

    def handle_indirect_ping(self, message, udp_cm):
        self._fd.on_indirect_ping(message)

    def handle_ack(self, message, udp_cm):
        self._fd.on_ack(message)
        print("ACK", message, udp_cm)

    def handle_nack(self, message, udp_cm):
        self._fd.on_nack(message)

    # gossip
    def handle_alive(self, message, udp_cm):
        self._gossiper.alive(message)

    def handle_suspect(self, message, udp_cm):
        self._gossiper.suspect(message)

    def handle_dead(self, message, udp_cm):
        self._gossiper.dead(message)

    def __str__(self):
        c = self.config
        return "<Cluster: {}:{}>".format(c.host, c.port)

    def __repr__(self):
        c = self.config
        return "<Cluster: {}:{}>".format(c.host, c.port)


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
