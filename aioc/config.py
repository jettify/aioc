import attr

@attr.s
class Config:
    host = attr.ib(default="localhost")
    port = attr.ib(default=9900)
    active_rwl = attr.ib(default=5)
    passive_rwl = attr.ib(default=3)
    active_veiw_size = attr.ib(default=5)
    passive_veiw_size = attr.ib(default=8)
    cluster_id = attr.ib(default="aiocluster")
    active_view_log_threshold = attr.ib(default=5)
    fanout_constant_addition = attr.ib(default=1)


    # join_timeout: float = 10
    # push_pull_interval: float = 15
    # gossip_interval: float = 0.25
    # gossip_nodes: int = 1
    # probe_interval: float = 5
    # retransmit_mult: int = 2
    # gossip_to_dead: int = 3600
    # probe_timeout: float = 1
    # suspicion_mult: float = 1
    # suspicion_max_timeout_mult: float = 1

    def with_replace(self, **kw):
        return attr.evolve(self, **kw)

# LAN = Config('localhost', 50001, 10, 15, 0.25, 1, 5, 2, 3600, 1, 1, 1)

