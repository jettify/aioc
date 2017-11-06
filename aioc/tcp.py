import asyncio
from collections import namedtuple

from .state import (decode_msg_size, encode_message, decode_message,
                    add_msg_size)


async def create_tcp_server(*, host='127.0.0.1', port=9999, loop=None):
    cm = TCPConnectionManager()
    server = await asyncio.start_server(
        cm.handle_connection, host, port, loop=loop)
    cm._server = server
    return cm


Connection = namedtuple("Connection", ["reader", "writer"])


class TCPConnectionManager:

    def __init__(self):
        self._server = None

    async def close(self):
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def _read_message(self, reader):
        size_data = await reader.readexactly(4)
        mg_size = decode_msg_size(size_data)
        raw_message = await reader.readexactly(mg_size)
        return raw_message

    async def _request(self, address, payload):
        h, p = address
        # TODO add timeout
        r, w = await asyncio.open_connection(host=h, port=p)
        try:
            w.write(payload)
            await w.drain()
            raw_message = await self._read_message(r)
        finally:
            w.close()
        return raw_message

    async def send_message(self, address, message):
        payload = encode_message(message)
        raw = add_msg_size(payload)
        raw_message = await self._request(address, raw)
        return decode_message(raw_message)

    async def send_raw_message(self, address, raw):
        raw_message = await self._request(address, raw)
        return decode_message(raw_message)

    async def handle_connection(self, reader, writer):
        conn = Connection(reader, writer)
        try:
            raw_message = await self._read_message(conn.reader)
            if self._hander:
                message = decode_message(raw_message)
                await self._hander(message, conn)
                await writer.drain()
        finally:
            conn.writer.close()

    def set_handler(self, hander):
        self._hander = hander
