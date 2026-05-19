"""Integration tests for sync/async client interop inside a Jupyter kernel.

Uses testbook to run code in a real IPython kernel, which has a running event loop —
the exact environment that motivated the nest_asyncio removal and per-loop isolation.
"""

from __future__ import annotations

import pickle
from collections.abc import Iterator
from pathlib import Path

import nbformat
import pytest
from testbook import testbook
from testbook.client import TestbookNotebookClient

from cognite.client import CogniteClient


@pytest.fixture
def test_notebook(tmp_path: Path, cognite_client: CogniteClient) -> Iterator:
    nb_path, config_path = tmp_path / "interop.ipynb", tmp_path / "config.pkl"
    nb_path.write_text(nbformat.writes(nbformat.v4.new_notebook()))
    config_path.write_bytes(pickle.dumps(cognite_client.config))

    with testbook(str(nb_path), execute=True, timeout=15) as tb:
        tb.inject(
            f"""
            import pickle
            from pathlib import Path
            from cognite.client import CogniteClient, AsyncCogniteClient

            config = pickle.loads(Path({str(config_path)!r}).read_bytes())
            sync_client = CogniteClient(config)
            async_client = AsyncCogniteClient(config)
            """
        )
        yield tb


class TestJupyterSyncAsyncInterop:
    """
    Most of these tests failed on version 8.0.0 through 8.0.6 with:
    RuntimeError: <asyncio.locks.Event object at 0x10... [unset]> is bound to a different event loop

    There was also a separate issue on 3.14, but that has no particular test case (as it was due
    to asyncio internals, and how we used to patch them with nest_asyncio)

    Each test gets a fresh Jupyter kernel (separate process), so global state like the httpx
    client cache and semaphore cache is naturally isolated between tests — no explicit cleanup needed.
    """

    def test_kernel_errors_are_not_swallowed(self, test_notebook: TestbookNotebookClient) -> None:
        # This looks stupid, but we just want to verify that exceptions raised in the kernel gets surfaced
        from testbook.exceptions import TestbookRuntimeError

        with pytest.raises(TestbookRuntimeError, match="ZeroDivisionError"):
            test_notebook.inject("3.14 / 0")

    def test_sync_then_async_then_sync(self, test_notebook: TestbookNotebookClient) -> None:
        test_notebook.inject(
            """
            sync_client.assets.list(limit=1)
            await async_client.time_series.list(limit=2)
            sync_client.events.list(limit=3)
            await async_client.assets.list(limit=1)
            sync_client.time_series.list(limit=2)
            """
        )

    def test_async_then_sync_then_async(self, test_notebook: TestbookNotebookClient) -> None:
        test_notebook.inject(
            """
            await async_client.assets.list(limit=1)
            sync_client.events.list(limit=2)
            await async_client.time_series.list(limit=3)
            sync_client.assets.list(limit=1)
            await async_client.events.list(limit=2)
            """
        )

    def test_threadpool_sync_calls(self, test_notebook: TestbookNotebookClient) -> None:
        test_notebook.inject(
            """
            from concurrent.futures import ThreadPoolExecutor

            with ThreadPoolExecutor(3) as ex:
                futures = [
                    ex.submit(sync_client.assets.list, limit=1),
                    ex.submit(sync_client.events.list, limit=2),
                    ex.submit(sync_client.time_series.list, limit=3),
                ]
                results = [f.result() for f in futures]
            assert len(results) == 3
            """
        )

    def test_async_then_threadpool_sync_then_async(self, test_notebook: TestbookNotebookClient) -> None:
        # This test ensures that that the semaphore does not get bound to a particular event loop,
        # which we used to do - now we cache it with a key including the event loop.
        test_notebook.inject(
            """
            from concurrent.futures import ThreadPoolExecutor

            await async_client.assets.list(limit=1)

            with ThreadPoolExecutor(3) as ex:
                futures = [
                    ex.submit(sync_client.assets.list, limit=1),
                    ex.submit(sync_client.events.list, limit=2),
                    ex.submit(sync_client.time_series.list, limit=3),
                ]
                results = [f.result() for f in futures]
            assert len(results) == 3

            await async_client.events.list(limit=1)
            """
        )
