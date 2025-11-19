import asyncio
from collections.abc import AsyncIterator, Coroutine, Iterator
from typing import Any, TypeVar

from cognite.client.utils._concurrency import ConcurrencySettings

_T = TypeVar("_T")
_SENTINEL = object()


class SyncIterator(Iterator[_T]):
    """A synchronous iterator that wraps an async iterator. Used in the sync Cognite client to
    avoid loading all items into memory before yielding them as it just wraps the async client."""

    def __init__(self, async_iter: AsyncIterator[_T]) -> None:
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
    executor = ConcurrencySettings._get_event_loop_executor()
    return executor.run_coro(coro)


if __name__ == "__main__":
    # Example usage:

    async def async_gen() -> AsyncIterator[int]:
        for i in range(5):
            await asyncio.sleep(0.1)
            yield i

    sync_iter = SyncIterator(async_gen())
    for x in sync_iter:
        print(x)  # noqa: T201

    # As opposed to:
    # async for x in async_gen():
    #     print(x)
