"""
===============================================================================
c4064290ea82271873bbc2c0a330fc30
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api.raw.databases import SyncRawDatabasesAPI
from cognite.client._sync_api.raw.rows import SyncRawRowsAPI
from cognite.client._sync_api.raw.tables import SyncRawTablesAPI
from cognite.client._sync_api_client import SyncAPIClient

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncRawAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient):
        self.__async_client = async_client
        self.databases = SyncRawDatabasesAPI(async_client)
        self.tables = SyncRawTablesAPI(async_client)
        self.rows = SyncRawRowsAPI(async_client)
