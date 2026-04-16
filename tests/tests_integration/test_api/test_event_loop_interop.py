"""Integration tests for sync/async client interop with real API calls.

Ensures connections and semaphores aren't cross-contaminated between event loops.
"""

from __future__ import annotations

import random
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest

from cognite.client import AsyncCogniteClient, CogniteClient, global_config
from cognite.client._http_client import _global_async_httpx_clients


@pytest.fixture(autouse=True)
def _clear_loop_caches() -> None:
    """Reset per-loop caches so each test starts with a clean slate (...and state lol)."""
    _global_async_httpx_clients.clear()
    for config in (
        global_config.concurrency_settings.general,
        global_config.concurrency_settings.datapoints,
        global_config.concurrency_settings.raw,
        global_config.concurrency_settings.data_modeling,
    ):
        config._semaphore_cache.clear()


def make_sync_api_call(client: CogniteClient) -> Any:
    limit = random.randint(1, 5)
    calls = [
        lambda: client.assets.list(limit=limit),
        lambda: client.events.list(limit=limit),
        lambda: client.time_series.list(limit=limit),
        lambda: client.iam.token.inspect(),
    ]
    return random.choice(calls)()


async def make_async_api_call(async_client: AsyncCogniteClient) -> Any:
    limit = random.randint(1, 5)
    calls = [
        lambda: async_client.assets.list(limit=limit),
        lambda: async_client.events.list(limit=limit),
        lambda: async_client.time_series.list(limit=limit),
        lambda: async_client.iam.token.inspect(),
    ]
    return await random.choice(calls)()


class TestSyncAsyncInterop:
    async def test_sync_then_async_then_sync(
        self, cognite_client: CogniteClient, async_client: AsyncCogniteClient
    ) -> None:
        make_sync_api_call(cognite_client)
        await make_async_api_call(async_client)
        make_sync_api_call(cognite_client)
        await make_async_api_call(async_client)
        make_sync_api_call(cognite_client)

    async def test_async_then_sync_then_async(
        self, cognite_client: CogniteClient, async_client: AsyncCogniteClient
    ) -> None:
        await make_async_api_call(async_client)
        make_sync_api_call(cognite_client)
        await make_async_api_call(async_client)
        make_sync_api_call(cognite_client)
        await make_async_api_call(async_client)


class TestThreadPoolExecutorSync:
    def test_concurrent_sync_calls(self, cognite_client: CogniteClient) -> None:
        with ThreadPoolExecutor(4) as ex:
            futures = [ex.submit(make_sync_api_call, cognite_client) for _ in range(4)]
            results = [f.result() for f in futures]
        assert len(results) == 4

    async def test_async_then_threadpool_sync_then_async(
        self, cognite_client: CogniteClient, async_client: AsyncCogniteClient
    ) -> None:
        await make_async_api_call(async_client)

        with ThreadPoolExecutor(3) as ex:
            futures = [ex.submit(make_sync_api_call, cognite_client) for _ in range(3)]
            results = [f.result() for f in futures]
        assert len(results) == 3

        await make_async_api_call(async_client)
