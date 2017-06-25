import asyncio

from .config import Config
from .net import ConnectionManager
from .utils import IdGenerator

from . import state as s


class Cluster:
    def __init__(self, config: Config):
        self._server = None
        self._cm = None
        self._node = s.Node(config.host, config.port)
        self._joined = asyncio.Future()
        self._closing = False
        self._active_view = {}
        self._passive_view = {}
        self._config = config
        self._id_generator = IdGenerator()

    @property
    def local_node(self):
        return self._node

    @property
    def nodes(self):
        return tuple([self.local_node] +
                     list(self._active_view().keys()) +
                     list(self._passive_view().keys()))

    @property
    def active_view(self):
        return tuple(self._active_view().keys())

    @property
    def passive_view(self):
        return tuple(self._passive_view().keys())

    async def boot(self):
        cm = ConnectionManager(self._node)
        server = await asyncio.start_server(cm.handle_connection,
                                            host=self.local_node.host,
                                            port=self.local_node.port)
        self._cm = cm
        self._server = server
        self._reader_task = asyncio.ensure_future(self.receive())


    async def join(self, node):
        msg = s.Join(1, self.local_node)
        await self._cm.send(node, msg)
        await self._joined

    async def shutdown(self):
        self._closing = True
        msg = s.Disconnect(1, self.local_node, True)
        for node in self._active_view:
            await self._cm.send(node, msg)

        for node in self._passive_view:
            await self._cm.send(node, msg)

        self._reader_task.cancel()
        try:
            await self._reader_task
        except asyncio.CancelledError:
            pass

    async def receive(self):
        while not self._closing:
            msg = await self._cm.queue.get()
            if isinstance(msg, s.Join):
                await self.handle_join(msg)

            elif isinstance(msg, s.JoinReply):
                await self.handle_join_response(msg)

            elif isinstance(msg, s.ForwardJoin):
                await self.handle_forward_join(msg)

            elif isinstance(msg, s.Neigbour):
                await self.handle_neigbour(msg)

            elif isinstance(msg, s.Shuffle):
                await self.handle_shuffle(msg)

            elif isinstance(msg, s.Disconnect):
                await self.handle_disconnect(msg)
            else:
                raise RuntimeError("xxx")

    def add_to_view(self, node):
        self._active_view[node] = 1

    def remove_from_view(self, node):
        self._active_view.pop(node)

    async def handle_join(self, msg):
        reply = s.JoinReply(1, self.local_node, 1)
        await self._cm.send(msg.sender, reply)
        forward = s.ForwardJoin(
            1, self.local_node, msg.sender, self._config.active_rwl)
        for node in self._active_view.keys():
            if node == self.local_node:
                continue
            await self._cm.send(node, forward)
        self.add_to_view(msg.sender)

    async def handle_join_response(self, msg):
        self.add_to_view(msg.sender)
        if not self._joined.done():
            self._joined.set_result(None)

    async def handle_forward_join(self, msg):
        next_ttl = msg.ttl - 1
        if next_ttl <= 0 or len(self._active_view) == 1:
            self.add_to_view(msg.joiner)
            reply = s.JoinReply(1, self.local_node, 1)
            await self._cm.send(msg.joiner, reply)
        else:
            self.add_to_passive(msg.joiner)
            forward = msg._replace(ttl=next_ttl)
            for node in self._active_view.keys():
                if node == self.local_node or node == msg.joiner:
                    continue
                await self._cm.send(node, forward)
                break

    async def handle_neighbour(self, msg):
        print(msg)

    async def handle_shuffle(self, msg):
        print(msg)

    async def handle_disconnect(self, msg):
        self._active_view.pop(msg.sender, None)
        if msg.leave:
            self._passive_view.pop(msg.sender, None)
            await self._cm.cleanup(msg.sender)

    async def close(self):
        await self.shutdown()
        await self._cm.close()
