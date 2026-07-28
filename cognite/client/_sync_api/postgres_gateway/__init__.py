"""
===============================================================================
506bda1e5a8fa5d128a4da3ae05bb18b
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api.postgres_gateway.tables import SyncTablesAPI
from cognite.client._sync_api.postgres_gateway.users import SyncUsersAPI
from cognite.client._sync_api_client import SyncAPIClient

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncPostgresGatewaysAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client
        self.users = SyncUsersAPI(async_client)
        self.tables = SyncTablesAPI(async_client)
