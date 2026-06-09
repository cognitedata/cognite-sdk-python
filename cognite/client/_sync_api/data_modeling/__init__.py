"""
===============================================================================
6defd31848f249cb0e2c41bc254d5b8b
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api.data_modeling.containers import SyncContainersAPI
from cognite.client._sync_api.data_modeling.data_models import SyncDataModelsAPI
from cognite.client._sync_api.data_modeling.files import SyncDataModelingFilesAPI
from cognite.client._sync_api.data_modeling.graphql import SyncDataModelingGraphQLAPI
from cognite.client._sync_api.data_modeling.instances import SyncInstancesAPI
from cognite.client._sync_api.data_modeling.records import SyncRecordsAPI
from cognite.client._sync_api.data_modeling.spaces import SyncSpacesAPI
from cognite.client._sync_api.data_modeling.statistics import SyncStatisticsAPI
from cognite.client._sync_api.data_modeling.streams import SyncStreamsAPI
from cognite.client._sync_api.data_modeling.time_series import SyncDataModelingTimeSeriesAPI
from cognite.client._sync_api.data_modeling.views import SyncViewsAPI
from cognite.client._sync_api_client import SyncAPIClient

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncDataModelingAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client
        self.containers = SyncContainersAPI(async_client)
        self.data_models = SyncDataModelsAPI(async_client)
        self.files = SyncDataModelingFilesAPI(async_client)
        self.time_series = SyncDataModelingTimeSeriesAPI(async_client)
        self.spaces = SyncSpacesAPI(async_client)
        self.views = SyncViewsAPI(async_client)
        self.instances = SyncInstancesAPI(async_client)
        self.graphql = SyncDataModelingGraphQLAPI(async_client)
        self.records = SyncRecordsAPI(async_client)
        self.statistics = SyncStatisticsAPI(async_client)
        self.streams = SyncStreamsAPI(async_client)
