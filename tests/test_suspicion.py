import time
import pytest

from aioc.state import Node
from aioc.suspicion import Suspicion


@pytest.mark.asyncio
async def test_suspicion_timeout_calculation(loop):

    def f():
        pass

    from_node = Node("127.0.0.1", 8000)
    s = Suspicion(from_node, 3, 2, 30, f, loop=loop)
    t = s.remaining_suspicion_time(0)
    assert t == 30

    s.confirm(Node("127.0.0.1", 8080))
    t = s.remaining_suspicion_time(2)
    assert t == 14

    s.confirm(Node("127.0.0.1", 8081))
    t = s.remaining_suspicion_time(3)
    assert t == pytest.approx(4.810524989903811)

    s.confirm(Node("127.0.0.1", 8082))
    t = s.remaining_suspicion_time(4)
    assert t == -2

    s.confirm(Node("127.0.0.1", 8083))
    s.confirm(Node("127.0.0.1", 8083))
    t = s.remaining_suspicion_time(5)
    assert t == -3
    s.stop()


def make_callable(loop):
    fut = loop.create_future()

    def f():
        t = time.time()
        fut.set_result(t)
        return f
    return fut, f


@pytest.mark.asyncio
async def test_suspicion_timer_no_confirmations(loop):
    from_node = Node("127.0.0.1", 8000)
    fut, f = make_callable(loop)
    min_time = 0.5
    max_time = 2
    start = time.time()
    s = Suspicion(from_node, 3, min_time, max_time, f, loop=loop)
    ts = await fut
    assert ts - start == pytest.approx(max_time, rel=1e-2)
    s.stop()


@pytest.mark.asyncio
async def test_suspicion_timer_max_confimations(loop):
    from_node = Node("127.0.0.1", 8000)
    fut, f = make_callable(loop)
    min_time = 0.5
    max_time = 2
    start = time.time()
    s = Suspicion(from_node, 3, min_time, max_time, f, loop=loop)
    s.confirm(Node("127.0.0.1", 8081))
    s.confirm(Node("127.0.0.1", 8082))
    s.confirm(Node("127.0.0.1", 8083))
    ts = await fut
    assert ts - start == pytest.approx(min_time, rel=1e-2)
    s.stop()


@pytest.mark.asyncio
async def test_suspicion_timer_one_confirmation(loop):
    from_node = Node("127.0.0.1", 8000)
    fut, f = make_callable(loop)
    min_time = 0.5
    max_time = 2
    start = time.time()
    s = Suspicion(from_node, 3, min_time, max_time, f, loop=loop)
    s.confirm(Node("127.0.0.1", 8081))
    s.confirm(from_node)

    ts = await fut
    assert ts - start == pytest.approx(1.250, rel=1e-2)
    s.stop()
