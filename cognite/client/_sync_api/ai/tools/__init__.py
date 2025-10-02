"""
===============================================================================
c07271fd270ca0c1a19ea1dd1cff15af
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Coroutine
from typing import TYPE_CHECKING, Any, TypeVar

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api.ai.tools.documents import SyncAIDocumentsAPI
from cognite.client.utils._concurrency import ConcurrencySettings

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient

_T = TypeVar("_T")


class SyncAIToolsAPI:
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient):
        self.__async_client = async_client
        self.documents = SyncAIDocumentsAPI(async_client)

    def _run_sync(self, coro: Coroutine[Any, Any, _T]) -> _T:
        executor = ConcurrencySettings._get_event_loop_executor()
        return executor.run_coro(coro)
