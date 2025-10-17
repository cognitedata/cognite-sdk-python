"""
===============================================================================
66e3c7c6a5b970455f6ecbbc6efaa5d2
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api.three_d.asset_mapping import Sync3DAssetMappingAPI
from cognite.client._sync_api.three_d.files import Sync3DFilesAPI
from cognite.client._sync_api.three_d.models import Sync3DModelsAPI
from cognite.client._sync_api.three_d.revisions import Sync3DRevisionsAPI
from cognite.client._sync_api_client import SyncAPIClient

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class Sync3DAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient):
        self.__async_client = async_client
        self.models = Sync3DModelsAPI(async_client)
        self.revisions = Sync3DRevisionsAPI(async_client)
        self.files = Sync3DFilesAPI(async_client)
        self.asset_mappings = Sync3DAssetMappingAPI(async_client)
