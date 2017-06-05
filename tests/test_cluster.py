import pytest
import asyncio

from aioc.cluster import Cluster
from aioc.config import LAN
from aioc.state import add_msg_size, encode_message, Ping
from aioc.tcp import TCPClient


class ClientUdpProtocol:
    def __init__(self, loop):
        self._loop = loop
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        print("received {addr}", data)

    def error_received(self, exc):
        print('Error received:', exc)

    def connection_lost(self, exc):
        print('closing transport', exc)

    def sentdto(self, msg):
        p = encode_message(msg)
        self.transport.sendto(add_msg_size(p))


@pytest.yield_fixture
def udp_client(loop):
    addr = ('localhost', 50001)

    def protocol_factory():
        return ClientUdpProtocol(loop)
    t = loop.create_task(
        loop.create_datagram_endpoint(protocol_factory, remote_addr=addr))
    transport, p = loop.run_until_complete(t)
    yield p
    transport.close()


@pytest.yield_fixture
def tcp_client(loop):
    client = TCPClient(loop)
    yield client


@pytest.mark.run_loop
async def test_ctor(loop, udp_client):
    cluster = Cluster(LAN, loop=loop)
    await cluster.boot()
    msg = (777, b'node_id')
    udp_client.sentdto(Ping(*msg))
    await asyncio.sleep(0.1, loop=loop)
    await cluster.leave()


@pytest.mark.run_loop
async def test_tcp_join(loop, tcp_client):
    c1_address = ('localhost', 50001)
    c1 = LAN.with_replace(host='localhost', port=50001)
    cluster1 = Cluster(c1, loop=loop)
    await cluster1.boot()
    configs = [LAN.with_replace(host='localhost', port=50002 + i)
               for i in range(5)]
    clusters = [Cluster(c, loop=loop) for c in configs]
    for cluster in clusters:
        await cluster.boot()
        await cluster.join(c1_address)

    await asyncio.sleep(1, loop=loop)

    for cluster in clusters:
        await cluster.leave()
    await cluster1.leave()


@pytest.mark.run_loop
async def test_update_node(loop, tcp_client):
    c1_address = ('localhost', 50001)
    c1 = LAN.with_replace(host='localhost', port=50001)
    cluster1 = Cluster(c1, loop=loop)
    incarnation = cluster1.local_node.incarnation
    await cluster1.boot()
    configs = [LAN.with_replace(host='localhost', port=50002 + i)
               for i in range(3)]
    clusters = [Cluster(c, loop=loop) for c in configs]
    for cluster in clusters:
        await cluster.boot()
        await cluster.join(c1_address)

    await cluster1.update_node('xxx')
    await asyncio.sleep(1, loop=loop)

    for cluster in clusters:
        node = cluster.get_node('{}:{}'.format(*c1_address))
        node.meta == 'xxx'
        node.incarnation > incarnation

    for cluster in clusters:
        await cluster.leave()
    await cluster1.leave()


@pytest.mark.run_loop
async def test_node_listener(loop, tcp_client):
    c1_address = ('localhost', 50001)
    c1 = LAN.with_replace(host='localhost', port=50001)
    c2 = LAN.with_replace(host='localhost', port=50002)

    cluster1 = Cluster(c1, loop=loop)
    cluster2 = Cluster(c2, loop=loop)

    async def node_join(event_type, node):
        print(event_type, node)
        assert event_type == "JOIN"

    cluster1.listener.add_handler(node_join)
    await cluster1.boot()
    await cluster2.boot()
    await cluster2.join(c1_address)

    await cluster1.leave()
    await cluster2.leave()
