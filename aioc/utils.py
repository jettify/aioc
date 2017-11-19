import asyncio  # noqa
import contextlib
from typing import Optional
OptLoop = Optional[asyncio.AbstractEventLoop]


class Ticker:

    def __init__(self, corofunc, interval, timeout_func=None,
                 loop: OptLoop=None) -> None:
        self._interval = interval
        self._loop = loop
        self._ticker_task = None
        self._closing = False
        self._ticker = corofunc
        self._timout_func = timeout_func or simple_timeout

    @property
    def closed(self):
        return self._ticker_task is None

    async def _tick(self):
        # TODO: add initial wait time to prevent all task to start
        # in same time
        while not self._closing:
            t_start = self._loop.time()

            try:
                await self._ticker()
            except Exception as e:
                # TODO: redo handling exceptions
                print(e)
                raise e

            t_stop = self._loop.time()
            t = self._timout_func(self._interval, t_start, t_stop)
            await asyncio.sleep(t, loop=self._loop)

    def start(self):
        self._ticker_task = asyncio.ensure_future(
            self._tick(), loop=self._loop)

    async def stop(self):
        self._closing = True
        if self._ticker_task is None:
            return
        # TODO: add timeout for more correct shutdown
        with contextlib.suppress(asyncio.CancelledError):
            await self._ticker_task
            self._ticker_task = None


def create_ticker(corofunc, interval: float, loop=None) -> Ticker:
    ticker = Ticker(corofunc, interval, loop=loop)
    ticker.start()
    return ticker


def simple_timeout(interval: float, tick_start: float,
                   tick_stop: float) -> float:
    t = max(interval - (tick_stop - tick_start), 0)
    return t


class LClock:

    def __init__(self, incarantion: int=1, sequence_num: int=1) -> None:
        self._incarnation = incarantion
        self._sequence_num = sequence_num

    @property
    def incarnation(self) -> int:
        return self._incarnation

    @property
    def sequence_num(self) -> int:
        return self._sequence_num

    def next_incarnation(self) -> int:
        self._incarnation += 1
        return self._incarnation

    def skip_incarnation(self, offset: int) -> int:
        self._incarnation += offset
        return self._incarnation

    def next_sequence_num(self) -> int:
        self._sequence_num += 1
        return self._sequence_num


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
