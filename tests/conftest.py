import pytest
from aioc.config import Config


@pytest.fixture
def loop(event_loop):
    # event_loop.set_debug(True)
    return event_loop


@pytest.fixture
def config():
    conf = Config(
        host='localhost',
        port=50001,
        join_timeout=10,
        push_pull_interval=3,
        gossip_interval=1,
        gossip_nodes=3,
        probe_interval=5,
        retransmit_mult=2,
        gossip_to_dead=3600,
        probe_timeout=1,
        suspicion_mult=1,
        suspicion_max_timeout_mult=1
    )
    return conf


pytest_plugins = []
