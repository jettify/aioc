import asyncio
from collections import namedtuple

from . import state
from .state import (decode_msg_size, encode_message, decode_message,
                    add_msg_size, PushPull)


async def create_tcp_server(*, host='127.0.0.1', port=9999, mlist,
                            gossiper,  loop=None):
    handler = TCPMessageHandler(mlist, gossiper, loop=loop)
    server = await asyncio.start_server(
        handler.handle_connection, host, port, loop=loop)
    return server, handler


async def read_message(reader):
    size_data = await reader.readexactly(4)
    mg_size = decode_msg_size(size_data)
    raw_message = await reader.readexactly(mg_size)
    return raw_message


Connection = namedtuple("Connection", ["reader", "writer"])


class TCPMessageHandler:

    def __init__(self, mlist, gossiper, loop):
        self._loop = loop
        self._mlist = mlist
        self._gossiper = gossiper

    async def handle_connection(self, reader, writer):
        conn = Connection(reader, writer)
        try:
            raw_message = await read_message(conn.reader)
            await self.handle_message(raw_message, conn)
            await writer.drain()
        finally:
            conn.writer.close()

    async def handle_message(self, raw_message, conn):
        message = decode_message(raw_message)
        if isinstance(message, state.Ping):
            await self.handle_ping(message, conn)

        elif isinstance(message, state.PushPull):
            await self.handle_push_pull(message, conn)

        elif isinstance(message, state.UserMsg):
            await self.handle_user(message, conn)
        else:
            await self.handle_unknown(message, conn)

    async def handle_ping(self, message, conn):
        sequence_num = message.sequence_num
        # TODO: handle ack payload correctly
        ack = state.AckResp(sequence_num, b'ping')
        raw = state.add_msg_size(state.encode_message(ack))
        conn.writer.write(raw)

    async def handle_user(self, message, conn):
        print(message, conn)

    async def handle_push_pull(self, message, conn):
        resp = PushPull(self._mlist.nodes, False)
        self._gossiper.merge(message)
        raw = state.add_msg_size(state.encode_message(resp))
        conn.writer.write(raw)

    async def handle_unknown(self, message, conn):
        print(message, conn)


class TCPClient:

    def __init__(self, loop):
        self._loop = loop

    async def request(self, address, payload):
        h, p = address
        # TODO add timeout
        r, w = await asyncio.open_connection(host=h, port=p, loop=self._loop)
        try:
            w.write(payload)
            await w.drain()
            raw_message = await read_message(r)
        finally:
            w.close()
        return raw_message

    async def send_message(self, address, message):
        payload = encode_message(message)
        raw = add_msg_size(payload)
        raw_message = await self.request(address, raw)
        return decode_message(raw_message)
