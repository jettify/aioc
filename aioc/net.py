import asyncio
from . import state


class UDPConnectionManager:

    def __init__(self, protocol):
        self._protocol = protocol

    async def send_message(self, address, *messages):
        raw = state.add_msg_size(state.encode_messages(*messages))
        self._protocol.sendto(raw, address)

    async def send_raw_message(self, address, raw):
        self._protocol.sendto(raw, address)

    def set_handler(self, handler):
        self._protocol.set_handler(handler)

    async def close(self):
        self._protocol.close()


class UDPServerProtocol(asyncio.Protocol):

    def __init__(self, loop):
        self._transport = None
        self._loop = loop
        self._handler = None

    def connection_made(self, transport):
        self._transport = transport
        super().connection_made(transport)

    def set_handler(self, handler):
        self._handler = handler

    def datagram_received(self, data, addr):
        if self._handler is None:
            return

        size_data = state.decode_msg_size(data)
        header = state.LENGTH_SIZE
        raw_message = data[header: header + size_data]
        self._handler(raw_message, addr, self)

    def connection_lost(self, exc):
        super().connection_lost(exc)

    def sendto(self, data, addr):
        self._transport.sendto(data, addr)

    def close(self):
        self._transport.close()
        self._transport = None


async def create_server(*, host='127.0.0.1', port=9999, mlist, loop):
    address = (host, port)
    _, protocol = await loop.create_datagram_endpoint(
        lambda: UDPServerProtocol(loop), local_addr=address)
    return UDPConnectionManager(protocol)
