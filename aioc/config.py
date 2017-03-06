from typing import NamedTuple


class _Config(NamedTuple):
    host: str
    port: int
    join_timeout: float = 10
    push_pull_interval: float = 15
    gossip_interval: float = 0.25
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


LAN = Config('localhost', 50001, 10, 15, 0.25, 1, 5, 2, 3600, 1, 1, 1)
