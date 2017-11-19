import asyncio  # noqa
import math
import time
from typing import Callable, Set

from .state import Node


Loop = asyncio.AbstractEventLoop


class Suspicion:
    """Suspicion manages the suspect timer for a node and provides an
    interface to accelerate the timeout as we get more independent
    confirmations that a node is suspect.
    """

    def __init__(self, from_node: Node, k: int, min_time: float,
                 max_time: float, fn: Callable, *, loop: Loop) -> None:
        # n is the number of independent confirmations we've seen.
        self._n: int = 0

        # k is the number of independent confirmations we'd like to see in
        # order to drive the timer to its minimum value.
        self._k = k

        # min is the minimum timer value.
        self._min_time = min_time

        # max is the maximum timer value.
        self._max_time = max_time

        # start captures the timestamp when we began the timer. This is used
        # so we can calculate durations to feed the timer during updates in
        # a way the achieves the overall time we'd like.
        self._start_time = time.time()

        # timer is the underlying timer that implements the timeout.
        t = self._max_time
        self._timer = create_timer(t, fn, loop)

        # f is the function to call when the timer expires. We hold on to this
        # because there are cases where we call it directly.
        self._timeout_fn = fn

        # confirmations is a map of "from" nodes that have confirmed a given
        # node is suspect. This prevents double counting.
        self._confirmations: Set[Node] = set([from_node])

    def remaining_suspicion_time(self, elapsed) -> float:
        """Takes the state variables of the suspicion
        timer and calculates the remaining time to wait before considering a
        node dead. The return value can be negative, so be prepared to fire
        the timer immediately in that case.
        """
        frac = math.log(self._n + 1) / math.log(self._k + 1)
        raw = self._max_time - frac * (self._max_time - self._min_time)
        timeout = max(raw, self._min_time)
        return timeout - elapsed

    def confirm(self, from_node: Node) -> bool:
        if self._n >= self._k:
            return False

        if from_node in self._confirmations:
            return False

        self._confirmations.add(from_node)
        self._n += 1
        timeout = self.check_timeout()
        if timeout > 0:
            self._timer.reschedule(timeout)
        else:
            self._timer.cancel()
            self._timeout_fn()
        return True

    def check_timeout(self) -> float:
        elapsed = time.time() - self._start_time
        remaining = self.remaining_suspicion_time(elapsed)
        return remaining

    def stop(self) -> None:
        self._timer.cancel()


class Timer:

    def __init__(self, timeout: float, callback: Callable, loop: Loop) -> None:
        self._loop = loop
        self._clb = callback
        self._handle = loop.call_later(timeout, callback)

    def reschedule(self, timeout):
        self._handle.cancel()
        self._handle = self._loop.call_later(timeout, self._clb)

    def cancel(self):
        self._handle.cancel()


def create_timer(timeout: float, callback: Callable, loop: Loop) -> Timer:
    return Timer(timeout, callback, loop)


def suspicion_timeout(suspicion_mult: float, n: int, interval: float) -> float:
    node_scale = max(1, math.log10(max(1, n)))
    t = suspicion_mult * node_scale * interval
    return t
