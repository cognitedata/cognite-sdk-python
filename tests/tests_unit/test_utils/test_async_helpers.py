import asyncio
import random
import time
from collections.abc import AsyncIterator

from cognite.client.utils._async_helpers import SyncIterator, async_timed_cache


class TestAsyncTimedCache:
    async def test_is_async_safe(self) -> None:
        hello = []

        @async_timed_cache(ttl=2)
        async def the_function(i: int) -> None:
            if random.random() < 0.5:
                await asyncio.sleep(0)
            hello.append(i)

        async def entry(i: int) -> int:
            await the_function(i)
            return i

        results = sorted(await asyncio.gather(*(entry(i) for i in range(10))))

        assert len(hello) == 1
        assert results == list(range(10))

        # Wait for TTL and retry
        time.sleep(2.01)
        await the_function(42)
        assert len(hello) == 2 and hello[-1] == 42


class TestAsyncIterator:
    def test_sync_iterator(self) -> None:
        async def async_gen(n) -> AsyncIterator[int]:
            for i in range(n):
                await asyncio.sleep(0)
                yield i

        sync_iter = SyncIterator(async_gen(5))
        for x, i in zip(sync_iter, range(5)):
            assert x == i

        assert list(SyncIterator(async_gen(5))) == list(range(5))
