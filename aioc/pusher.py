from .state import PushPull
import time


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
        try:
            # TODO: add timeout
            resp = await self._tcp.send_message(address, msg)
        except OSError as e:
            print(e)
        else:
            self._gossiper.merge(resp)

    async def push_pull(self):
        """push_pull is invoked periodically to randomly perform a complete state
        exchange. Used to ensure a high level of convergence, but is also
        reasonably expensive as the entire state of this node is exchanged
        with the other node.
        """

        def f(meta):
            return not meta.is_local

        metas = self._mlist.kselect(1, f)
        for meta in metas:
            await self.push_pull_address(meta.node)

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
