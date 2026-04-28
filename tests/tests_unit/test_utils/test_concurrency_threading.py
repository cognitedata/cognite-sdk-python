"""Tests for concurrency primitives in multi-threaded environments.

The SDK's sync client submits coroutines to a background event loop from caller threads.
Semaphores are loop-bound, so each loop must get its own semaphore instance — we verify
that the (operation, project, loop) cache key enforces this correctly.
"""

from __future__ import annotations

import asyncio
import random
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest
from httpx import Response
from pytest_httpx import HTTPXMock

from cognite.client import CogniteClient
from cognite.client.utils._async_helpers import run_sync
from cognite.client.utils._concurrency import (
    EventLoopThreadExecutor,
    _get_event_loop_executor,
)
from tests.utils import fresh_concurrency_state


class TestPerLoopSemaphoreIsolation:
    def test_cache_keyed_by_loop_and_project(self) -> None:
        sems: set[asyncio.BoundedSemaphore] = set()

        with fresh_concurrency_state() as cs:

            def run_for_both_projects() -> None:
                async def get() -> tuple[asyncio.BoundedSemaphore, asyncio.BoundedSemaphore, asyncio.BoundedSemaphore]:
                    a = cs.general._semaphore_factory("read", "proj-1")
                    b = cs.general._semaphore_factory("read", "proj-22")
                    c = cs.general._semaphore_factory("read", "proj-1")
                    return a, b, c

                a, b, c = asyncio.run(get())
                sems.update([a, b, c])

            t1 = threading.Thread(target=run_for_both_projects)
            t2 = threading.Thread(target=run_for_both_projects)
            t1.start()
            t2.start()
            t1.join()
            t2.join()

        # We expect 4 (not 6), as 'c is a' should be true:
        assert len(sems) == 4

    def test_background_loop_and_asyncio_run_loop_get_distinct_semaphores(self) -> None:
        """The sync client's background loop and a direct asyncio.run() caller must never share semaphores.

        This is the concrete sync-client-plus-async-client coexistence scenario: a semaphore
        created on the background EventLoopThreadExecutor loop and one created on a fresh loop
        (e.g. an async test or Jupyter cell) are keyed to different loops and must be different objects.
        """
        with fresh_concurrency_state() as cs:

            async def factory_coro() -> asyncio.BoundedSemaphore:
                return cs.general._semaphore_factory("read", "shared-project")

            # Background (sync-client) loop:
            bg_sem = run_sync(factory_coro())
            # Fresh loop (async-client / asyncio.run path):
            fresh_sem = asyncio.run(factory_coro())

        assert bg_sem is not fresh_sem


class TestSyncClientMultiThreadedEnv:
    """
    The (sync) CogniteClient uses the helper function 'run_sync' internally to wrap the
    AsyncCogniteClient. Thus we run tests against said helper function
    """

    def test_concurrent_run_sync_calls_all_return_correct_results(self) -> None:
        """Verifies that multiple different threads submitting coroutines all get the right results back."""

        async def identity(x: int) -> int:
            await asyncio.sleep(random.uniform(0, 0.02))
            return x

        # This is a typical construct our users would use:
        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(run_sync, identity(i)) for i in range(50)]
            results = [f.result() for f in futures]

        assert results == list(range(50))

    def test_run_sync_propagates_exceptions_to_mainthread(self) -> None:
        async def boom() -> None:
            raise ValueError("from async")

        with pytest.raises(ValueError, match="from async"):
            run_sync(boom())

    def test_run_sync_propagates_exception_from_thread_pool(self) -> None:
        async def boom() -> None:
            raise RuntimeError("thread-propagated")

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(run_sync, boom()) for _ in range(2)]
            for f in futures:
                with pytest.raises(RuntimeError, match="thread-propagated"):
                    f.result()

    def test_semaphore_limits_concurrent_operations_via_background_loop(
        self, cognite_client: CogniteClient, httpx_mock: HTTPXMock
    ) -> None:
        """Backpressure (semaphores) works end-to-end: 10 concurrent sync API calls from a thread pool
        produce at most N (e.g., 3 in this test) in-flight HTTP requests at any time."""
        max_concurrent_requests = 3
        active, peak = 0, 0

        async def slow_response(request: Any) -> Response:
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(random.uniform(0, 0.05))
            active -= 1
            return Response(200, json={"items": []})

        httpx_mock.add_callback(slow_response, method="POST", url=re.compile(r".*"), is_reusable=True)

        with fresh_concurrency_state() as cs:
            cs.general.read = max_concurrent_requests
            with ThreadPoolExecutor(max_workers=10) as pool:
                # All these retrieve methods use the same 'general read' semaphore:
                futures = (
                    [pool.submit(cognite_client.assets.retrieve, id=i) for i in range(3)]
                    + [pool.submit(cognite_client.events.retrieve, id=i) for i in range(4)]
                    + [pool.submit(cognite_client.files.retrieve, id=i) for i in range(3)]
                )
                for f in futures:
                    f.result()

        assert peak == max_concurrent_requests


class TestEventLoopThreadExecutorLifecycle:
    def test_fresh_executor_starts_and_runs(self) -> None:
        ex = EventLoopThreadExecutor(daemon=True)
        ex.start()
        try:
            assert ex.is_alive()
            result = ex.run_coro(asyncio.sleep(0, result=42))
            assert result == 42
        finally:
            ex.stop()

    def test_run_coro_with_timeout(self) -> None:
        ex = EventLoopThreadExecutor(daemon=True)
        ex.start()

        async def slow() -> None:
            # Wrapping in wait_for ensures clean cancellation when the timeout fires:
            await asyncio.wait_for(asyncio.sleep(10), timeout=0.01)

        try:
            with pytest.raises(asyncio.TimeoutError):
                ex.run_coro(slow(), timeout=0.05)
        finally:
            ex.stop()

    def test_singleton_init_is_thread_safe(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Bug in 8.0.0 to 8.2.0: Many threads racing on the very first _get_event_loop_executor()
        call did not get the same instance (thread), leading to e.g. broken concurrency limits."""
        import cognite.client.utils._concurrency as conc

        # Force the lazy-init branch by removing any pre-existing singleton:
        executor_attribute = "_INTERNAL_EVENT_LOOP_THREAD_EXECUTOR_SINGLETON"
        monkeypatch.delattr(conc, executor_attribute, raising=False)

        results: list[Any] = []
        barrier = threading.Barrier(10)

        def get_executor() -> None:
            barrier.wait()  # maximize the race window
            results.append(_get_event_loop_executor())

        threads = [threading.Thread(target=get_executor) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(set(results)) == 1, "init race produced multiple executor instances"

        # Make sure that if someone renames executor_attribute, we fail here,
        # getattr will raise when not given a default. So - DO NOT rewrite this to
        # conc._INTERNAL_EVENT_LOOP_THREAD_EXECUTOR_SINGLETON:
        assert getattr(conc, executor_attribute) is results[0]


class TestFreezeRaceUnderThreads:
    def test_concurrent_factory_calls_all_succeed_and_freeze(self) -> None:
        """Multiple threads calling _semaphore_factory concurrently must not raise or corrupt state."""
        errors: list[BaseException] = []

        with fresh_concurrency_state() as cs:

            def run_factory() -> None:
                try:

                    async def call() -> None:
                        cs.general._semaphore_factory("read", "proj-race")

                    asyncio.run(call())
                except Exception as e:
                    errors.append(e)

            threads = [threading.Thread(target=run_factory) for _ in range(6)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            assert errors == [], f"Unexpected errors: {errors}"
            # Cache should have one entry per unique loop (one per thread):
            assert len(cs.general._semaphore_cache) == 6
