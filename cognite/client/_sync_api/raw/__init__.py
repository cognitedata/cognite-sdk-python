"""
===============================================================================
c4064290ea82271873bbc2c0a330fc30
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Coroutine
from typing import TYPE_CHECKING, Any, TypeVar

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api.raw.databases import SyncRawDatabasesAPI
from cognite.client._sync_api.raw.rows import SyncRawRowsAPI
from cognite.client._sync_api.raw.tables import SyncRawTablesAPI
from cognite.client.utils._concurrency import ConcurrencySettings

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient

_T = TypeVar("_T")


class SyncRawAPI:
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient):
        self.__async_client = async_client
        self.databases = SyncRawDatabasesAPI(async_client)
        self.tables = SyncRawTablesAPI(async_client)
        self.rows = SyncRawRowsAPI(async_client)

    def _run_sync(self, coro: Coroutine[Any, Any, _T]) -> _T:
        executor = ConcurrencySettings._get_event_loop_executor()
        return executor.run_coro(coro)
