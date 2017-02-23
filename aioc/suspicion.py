import math
import time


class Suspicion:
    """Suspicion manages the suspect timer for a node and provides an
    interface to accelerate the timeout as we get more independent
    confirmations that a node is suspect.
    """

    def __init__(self, k, min_time, max_time, fn, loop) -> None:
        # n is the number of independent confirmations we've seen.
        self._n = 0

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
        t = self._min_time
        self.timer = create_timer(t, fn, loop)

        # f is the function to call when the timer expires. We hold on to this
        # because there are cases where we call it directly.
        self._timeout_fn = fn

        # confirmations is a map of "from" nodes that have confirmed a given
        # node is suspect. This prevents double counting.
        self._confirmations = {}

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

    def confirm(self, from_node) -> bool:
        if self._n >= self._k:
            return False

        if from_node in self._confirmations:
            return False

        self._confirmations[from_node] = 1
        self._n += 1
        return True

    def check_timeout(self) -> bool:
        elapsed = time.time() - self._start_time
        remaining = self.remaining_suspicion_time(elapsed)
        return remaining > 0


class Timer:

    def __init__(self, timeout, callback, loop):
        self._loop = loop
        self._clb = callback
        self._handle = loop.call_later(timeout, callback)

    def reschedule(self, timeout):
        self._hander.cancel()
        self._handle = self._loop.call_later(timeout, self._clb)

    def cancel(self):
        self._hander.cancel()


def create_timer(timeout, callback, loop):
    return Timer(timeout, callback, loop)


def suspicion_timeout(suspicion_mult, n, interval):
    node_scale = max(1, math.log10(max(1, n)))
    t = suspicion_mult * node_scale * interval
    return t
