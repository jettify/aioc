import asyncio
from . import state


def send_udp_messages(udp_server, address, *messages):
    raw = state.add_msg_size(state.encode_messages(*messages))
    host, port = address.split(":")
    addr = (host, int(port))
    udp_server.sendto(raw, addr)


class UDPServerProtocol(asyncio.Protocol):

    def __init__(self, loop):
        self._transport = None
        self._closing = False
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
        self._closing = True

    async def wait_closed(self):
        self._transport.close()
        self._transport = None


async def create_server(*, host='127.0.0.1', port=9999, mlist, loop):
    address = (host, port)
    _, protocol = await loop.create_datagram_endpoint(
        lambda: UDPServerProtocol(loop), local_addr=address)
    return protocol


class MessageHandler:

    def __init__(self, mlist, gossiper, failure_detector, loop):
        self._loop = loop
        self._mlist = mlist
        self._gossiper = gossiper
        self._failure_detector = failure_detector

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

    def handle_message(self, addr, message, protocol):
        if isinstance(message, state.Ping):
            self.handle_ping(message, addr, protocol)

        elif isinstance(message, state.IndirectPingReq):
            self.handle_indirect_ping(message, addr)

        elif isinstance(message, state.AckResp):
            self.handle_ack(message, addr, protocol)

        elif isinstance(message, state.NackResp):
            self.handle_nack(message, addr, protocol)

        elif isinstance(message, state.Alive):
            self.handle_alive(message, addr, protocol)

        else:
            raise RuntimeError("Can not handle message", message)

    # fe
    def handle_ping(self, message, addr, protocol):
        self._failure_detector.on_ping(message)

    def handle_indirect_ping(self, message, node, protocol):
        self._failure_detector.on_indirect_ping(message)
        print(message, node)
        pass

    def handle_ack(self, message, node, protocol):
        self._failure_detector.on_ack(message)
        print("ACK", message, node, protocol)

    def handle_nack(self, message, node, protocol):
        self._failure_detector.on_ping(message)

    # gossip
    def handle_alive(self, message, node, protocol):
        self._gossiper.alive(message)

    def handle_suspect(self, message, node, protocol):
        self._gossiper.suspect(message)

    def handle_dead(self, message, node, protocol):
        self._gossiper.dead(message)

    # user
    def handle_user(self, message, node, protocol):
        print(message, node)
