from .state import PushPull, ALIVE


__all__ = ('Pusher',)


class Pusher:

    def __init__(self, mlist, gossiper, tcp, loop):
        self._loop = loop
        self._mlist = mlist
        self._gossiper = gossiper
        self._tcp = tcp

    async def push_pull_address(self, address):
        metas = list(self._mlist._members.values())
        msg = PushPull(self._mlist.local_node, metas, True)
        resp = await self._tcp.send_message(address, msg)
        self._gossiper.merge(resp)

    async def push_pull(self):
        def filter_func(node_meta):
            if node_meta.node == self._mlist.local_node:
                return False
            if node_meta.status != ALIVE:
                return False
            return True
        node_meta = self._mlist.kselect(1, filter_func)
        await self.push_pull_address(node_meta.node)

    async def join(self, *hosts) -> int:
        success = 0
        for h in hosts:
            try:
                await self.push_pull_address(h)
                success += 1
            except OSError as e:
                # TODO: add proper error handling in separate function
                print(e)
        assert success > 0
