from __future__ import annotations

from collections.abc import Iterator
from random import uniform


class Backoff(Iterator[float]):
    """Iterator that emits how long to wait, according to the "Full Jitter" approach
    described in this post: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/

    Args:
        multiplier (float): No description.
        max_wait (float): No description.
        base (int): No description."""

    def __init__(self, multiplier: float = 0.5, max_wait: float = 60.0, base: int = 2) -> None:
        self._multiplier = multiplier
        self._max_wait = max_wait
        self._base = base
        self._past_attempts = 0

    def __next__(self) -> float:
        # 100 is an arbitrary limit at which point most sensible parameters are likely to
        # be capped by max anyway.
        wait = uniform(0, min(self._multiplier * (self._base ** min(100, self._past_attempts)), self._max_wait))
        self._past_attempts += 1
        return wait

    def reset(self) -> None:
        self._past_attempts = 0

    def has_progressed(self) -> bool:
        return self._past_attempts > 0
