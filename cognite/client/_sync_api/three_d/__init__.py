"""
===============================================================================
66e3c7c6a5b970455f6ecbbc6efaa5d2
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Coroutine
from typing import TYPE_CHECKING, Any, TypeVar

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api.three_d.asset_mapping import SyncThreeDAssetMappingAPI
from cognite.client._sync_api.three_d.files import SyncThreeDFilesAPI
from cognite.client._sync_api.three_d.models import SyncThreeDModelsAPI
from cognite.client._sync_api.three_d.revisions import SyncThreeDRevisionsAPI
from cognite.client.utils._concurrency import ConcurrencySettings

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient

_T = TypeVar("_T")


class SyncThreeDAPI:
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient):
        self.__async_client = async_client
        self.models = SyncThreeDModelsAPI(async_client)
        self.revisions = SyncThreeDRevisionsAPI(async_client)
        self.files = SyncThreeDFilesAPI(async_client)
        self.asset_mappings = SyncThreeDAssetMappingAPI(async_client)

    def _run_sync(self, coro: Coroutine[Any, Any, _T]) -> _T:
        executor = ConcurrencySettings._get_event_loop_executor()
        return executor.run_coro(coro)
