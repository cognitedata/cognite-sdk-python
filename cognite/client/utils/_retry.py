from __future__ import annotations

from collections.abc import Iterator
from random import uniform


class Backoff(Iterator[float]):
    """Iterator that emits how long to wait, according to the "Full Jitter" approach
    described in this post: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/

    Args:
        multiplier (float): Scales the exponential curve; the cap after n attempts is ``multiplier * base**n``.
        max_wait (float): Hard ceiling on the sampled wait in seconds.
        base (int): Exponential base; 2 gives classic doubling behaviour.
        min_wait (float): Lower bound of the uniform sample, useful when an immediate retry is undesirable."""

    def __init__(self, multiplier: float = 0.5, max_wait: float = 60.0, base: int = 2, min_wait: float = 0.0) -> None:
        self._multiplier = multiplier
        self._max_wait = max_wait
        self._base = base
        self._min_wait = min_wait
        self._past_attempts = 0

    def __next__(self) -> float:
        # 100 is an arbitrary limit at which point most sensible parameters are likely to
        # be capped by max anyway.
        base = self._multiplier * (self._base ** min(100, self._past_attempts))
        cap = max(self._min_wait, min(base, self._max_wait))
        wait = uniform(self._min_wait, cap)
        self._past_attempts += 1
        return wait

    def reset(self) -> None:
        self._past_attempts = 0

    def has_progressed(self) -> bool:
        return self._past_attempts > 0
