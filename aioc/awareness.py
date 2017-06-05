import logging

log = logging.getLogger(__name__)


class Awareness:

    def __init__(self, max: int) -> None:
        # max is the upper threshold for the timeout scale (the score will be
        # constrained to be from 0 <= score < max).
        self._max = max
        # score is the current awareness score. Lower values are healthier and
        # zero is the minimum value.
        self._score = 0

    def apply_delta(self, delta: int) -> None:
        self._score += delta

        if self._score < 0:
            self._score = 0
        elif self._score > (self._max - 1):
            self._score = self._max - 1

        log.info(f'mlist health: {self._score}')

    @property
    def health_score(self) -> int:
        return self._score

    def scale_timeout(self, timeout) -> int:
        return timeout * (self._score + 1)
