import asyncio
from collections.abc import AsyncIterator, Iterator
from typing import TypeVar

from cognite.client.utils._concurrency import ConcurrencySettings

T = TypeVar("T")
_SENTINEL = object()


class SyncIterator(Iterator[T]):
    """A synchronous iterator that wraps an async iterator. Used in the sync Cognite client to
    avoid loading all items into memory before yielding them as it just wraps the async client."""

    def __init__(self, async_iter: AsyncIterator[T]) -> None:
        self._async_iter = async_iter
        self._run_coro = ConcurrencySettings._get_event_loop_executor().run_coro

    def __iter__(self) -> Iterator[T]:
        return self

    def __next__(self) -> T:
        async def get_next() -> T:
            try:
                return await self._async_iter.__anext__()
            except StopAsyncIteration:
                # A coroutine can not raise StopIteration:
                return _SENTINEL  # type: ignore[return-value]

        if (result := self._run_coro(get_next())) is _SENTINEL:
            raise StopIteration
        return result


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
