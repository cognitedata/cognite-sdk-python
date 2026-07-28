"""
===============================================================================
9f3dd7689d375651560a7f9b91cb94e7
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api.ai.tools import SyncAIToolsAPI
from cognite.client._sync_api_client import SyncAPIClient

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncAIAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client
        self.tools = SyncAIToolsAPI(async_client)
