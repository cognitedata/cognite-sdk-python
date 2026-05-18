"""
===============================================================================
ac5c773c42b1b74b30604b81848f4e82
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Coroutine, Iterator  # noqa: F401
from typing import TYPE_CHECKING, Any, overload  # noqa: F401

from cognite.client import AsyncCogniteClient
from cognite.client._api_client import APIClient  # noqa: F401
from cognite.client._sync_api.data_modeling.containers import SyncContainersAPI
from cognite.client._sync_api.data_modeling.data_models import SyncDataModelsAPI
from cognite.client._sync_api.data_modeling.graphql import SyncDataModelingGraphQLAPI
from cognite.client._sync_api.data_modeling.instances import SyncInstancesAPI
from cognite.client._sync_api.data_modeling.records import SyncRecordsAPI
from cognite.client._sync_api.data_modeling.spaces import SyncSpacesAPI
from cognite.client._sync_api.data_modeling.statistics import SyncStatisticsAPI
from cognite.client._sync_api.data_modeling.streams import SyncStreamsAPI
from cognite.client._sync_api.data_modeling.views import SyncViewsAPI
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.utils._async_helpers import SyncIterator, run_sync  # noqa: F401
from cognite.client.utils._concurrency import _get_event_loop_executor  # noqa: F401

if TYPE_CHECKING:
    import asyncio  # noqa: F401

    import pandas as pd  # noqa: F401
from typing import Literal  # noqa: F401

from cognite.client.config import ClientConfig  # noqa: F401


class SyncDataModelingAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client
        self.containers = SyncContainersAPI(async_client)
        self.data_models = SyncDataModelsAPI(async_client)
        self.spaces = SyncSpacesAPI(async_client)
        self.views = SyncViewsAPI(async_client)
        self.instances = SyncInstancesAPI(async_client)
        self.graphql = SyncDataModelingGraphQLAPI(async_client)
        self.statistics = SyncStatisticsAPI(async_client)
        self.records = SyncRecordsAPI(async_client)
        self.streams = SyncStreamsAPI(async_client)
