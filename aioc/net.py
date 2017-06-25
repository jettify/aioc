import asyncio

from aioc import state
from aioc.state import add_msg_size, decode_msg_size, LENGTH_SIZE


async def read_message(reader):
    size_data = await reader.readexactly(LENGTH_SIZE)
    mg_size = decode_msg_size(size_data)
    raw_message = await reader.readexactly(mg_size)
    msg = state.decode_message(raw_message)
    return msg


class Connection:
    def __init__(self, reader, writer, manager):
        self._reader = reader
        self._writer = writer
        self._reader_task = asyncio.ensure_future(self._read_data())
        self._closing = False
        self._manager = manager

    async def _read_data(self):
        while not self._closing:
            msg = await read_message(self._reader)
            print("---------------")
            print(msg)
            await self._manager.queue.put(msg)

    def send(self, message):
        raw_payload = state.encode_message(message)
        self._writer.write(add_msg_size(raw_payload))

    async def close(self):
        self._closing = True
        self._reader_task.cancel()
        try:
            await self._reader_task
        except asyncio.CancelledError:
            pass


class ConnectionManager:

    def __init__(self, local_node):
        self._connections = {}
        self._queue = asyncio.Queue()
        self._closing = True
        self._local_node = local_node

    @property
    def queue(self):
        return self._queue

    async def cleanup(self, node):
        conn = self._connections.pop(node, None)
        if conn:
            await conn.close()

    async def handle_connection(self, reader, writer):
        hello = await read_message(reader)
        if not isinstance(hello, state.Hello):
            data = b"bad init line"
            writer.write(add_msg_size(data))
            await writer.drain()
            writer.close()
            return

        conn = Connection(reader, writer, self)
        self._connections[hello.sender] = conn

    async def _create_connection(self, node):
        reader, writer = await asyncio.open_connection(node.host, node.port)
        data = state.encode_message(state.Hello(1, self._local_node))
        writer.write(add_msg_size(data))
        await writer.drain()
        conn = Connection(reader, writer, self)
        self._connections[node] = conn
        return conn

    async def send(self, node, message):
        if node in self._connections:
            conn = self._connections[node]
        else:
            conn = await self._create_connection(node)
        conn.send(message)

    async def close(self):
        self._closing = True
        for node, conn in self._connections.items():
            await conn.close()
