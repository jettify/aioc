import math

from .state import encode_message, LENGTH_SIZE


class DisseminationQueue:

    def __init__(self, mlist, retransmit_mult):
        self._mlist = mlist
        self._queue = []
        self._factor = retransmit_mult

    def put(self, message, waiter=None):
        for i, (attempts, existing_msg, fut) in enumerate(self._queue):
            if self._invalidates(message, existing_msg):
                if (fut is not None) and (not fut.cancelled()):
                    fut.set_result(True)
                self._queue[i] = None
        self._queue = [q for q in self._queue if q is not None]
        attempts = 0
        self._queue.append((attempts, message, waiter))

    def _invalidates(self, a, b):
        return a.node == b.node

    def get_update_up_to(self, bytes_available):
        buffers = []
        factor = self._mlist.config.retransmit_mult

        num_nodes = self._mlist.num_nodes
        limit = retransmit_limit(factor, num_nodes)
        for i, (attempts, msg, fut) in enumerate(self._queue):
            raw_payload = encode_message(msg)
            if (len(raw_payload) + LENGTH_SIZE) <= bytes_available:
                buffers.append(raw_payload)
                bytes_available -= (len(raw_payload) + LENGTH_SIZE)

                if limit <= attempts:
                    self._queue[i] = None
                    if (fut is not None) and (not fut.cancelled()):
                        fut.set_result(True)
                else:
                    self._queue[i] = (attempts + 1, msg, fut)

        self._queue = sorted([q for q in self._queue if q is not None],
                             key=lambda i: i[0])
        return buffers


def retransmit_limit(retransmit_mult, num_nodes):
    node_scale = math.ceil(math.log(num_nodes + 1))
    return node_scale * retransmit_mult
