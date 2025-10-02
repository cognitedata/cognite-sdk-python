"""
===============================================================================
c07271fd270ca0c1a19ea1dd1cff15af
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api.ai.tools.documents import SyncAIDocumentsAPI
from cognite.client._sync_api_client import SyncAPIClient

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncAIToolsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient):
        self.__async_client = async_client
        self.documents = SyncAIDocumentsAPI(async_client)
