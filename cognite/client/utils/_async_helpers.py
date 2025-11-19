import asyncio
import functools
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine, Hashable, Iterator
from typing import Any, ParamSpec, TypeVar

_T = TypeVar("_T")
_P = ParamSpec("_P")

_SENTINEL = object()


class SyncIterator(Iterator[_T]):
    """A synchronous iterator that wraps an async iterator. Used in the sync Cognite client to
    avoid loading all items into memory before yielding them as it just wraps the async client."""

    def __init__(self, async_iter: AsyncIterator[_T]) -> None:
        from cognite.client.utils._concurrency import ConcurrencySettings

        self._async_iter = async_iter
        self._run_coro = ConcurrencySettings._get_event_loop_executor().run_coro

    def __iter__(self) -> Iterator[_T]:
        return self

    def __next__(self) -> _T:
        async def get_next() -> _T:
            try:
                return await self._async_iter.__anext__()
            except StopAsyncIteration:
                # A coroutine can not raise StopIteration:
                return _SENTINEL  # type: ignore[return-value]

        if (result := self._run_coro(get_next())) is _SENTINEL:
            raise StopIteration
        return result


def run_sync(coro: Coroutine[_T, Any, _T]) -> _T:
    from cognite.client.utils._concurrency import ConcurrencySettings

    executor = ConcurrencySettings._get_event_loop_executor()
    return executor.run_coro(coro)


def async_cache(func: Callable[..., Awaitable[_T]]) -> Callable[..., Awaitable[_T]]:
    lock = asyncio.Lock()
    cache: dict[tuple[Hashable, ...], _T] = {}

    @functools.wraps(func)
    async def wrapper(*args: Hashable, **kwargs: Any) -> _T:
        if kwargs:
            raise TypeError(f"{func.__name__}() does not support keyword arguments with async_cache")

        async with lock:
            if args in cache:
                return cache[args]

            result = cache[args] = await func(*args)
            return result

    return wrapper


def async_timed_cache(ttl: int = 5) -> Callable[[Callable[_P, Awaitable[_T]]], Callable[_P, Awaitable[_T]]]:
    """
    An asyncio-safe timed cache decorator for an async function. Accepts a time-to-live (TTL) in seconds.

    Note:
        This implementation caches a single return value for the function,
        ignoring any arguments passed to it for caching purposes. A call with any
        arguments after the TTL expires will refresh this single cached value.
    """

    def decorator(func: Callable[_P, Awaitable[_T]]) -> Callable[_P, Awaitable[_T]]:
        lock = asyncio.Lock()
        value: _T = None  # type: ignore [assignment]
        last_update_time = 0.0

        @functools.wraps(func)
        async def wrapper(*a: _P.args, **kw: _P.kwargs) -> _T:
            nonlocal value, last_update_time

            async with lock:
                # Check if a valid, non-expired value exists in the cache
                if (current_time := time.time()) - last_update_time < ttl:
                    return value

                # If cache is empty or expired, call the function to get a new value
                value, last_update_time = await func(*a, **kw), current_time
                return value

        return wrapper

    return decorator
