"""
===============================================================================
a13269ade1cded310610304c48e405b6
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api.hosted_extractors.destinations import SyncDestinationsAPI
from cognite.client._sync_api.hosted_extractors.jobs import SyncJobsAPI
from cognite.client._sync_api.hosted_extractors.mappings import SyncMappingsAPI
from cognite.client._sync_api.hosted_extractors.sources import SyncSourcesAPI
from cognite.client._sync_api_client import SyncAPIClient

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncHostedExtractorsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient):
        self.__async_client = async_client
        self.sources = SyncSourcesAPI(async_client)
        self.destinations = SyncDestinationsAPI(async_client)
        self.jobs = SyncJobsAPI(async_client)
        self.mappings = SyncMappingsAPI(async_client)
