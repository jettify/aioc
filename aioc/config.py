from typing import NamedTuple


class _Config(NamedTuple):
    host: str
    port: int
    join_timeout: float = 10
    push_pull_interval: float = 3
    gossip_interval: float = 1
    gossip_nodes: int = 1
    probe_interval: float = 5
    retransmit_mult: int = 2
    gossip_to_dead: int = 3600
    probe_timeout: float = 1
    suspicion_mult: float = 1
    suspicion_max_timeout_mult: float = 1


class Config(_Config):

    def with_replace(self, **kw):
        return self._replace(**kw)


LAN = Config(
    host='localhost',
    port=50001,
    join_timeout=10,
    push_pull_interval=15,
    gossip_interval=0.25,
    gossip_nodes=1,
    probe_interval=5,
    retransmit_mult=2,
    gossip_to_dead=3600,
    probe_timeout=1,
    suspicion_mult=1,
    suspicion_max_timeout_mult= 1
)
