from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Literal

from cognite.client._api.data_modeling.containers import ContainersAPI
from cognite.client._api.data_modeling.data_models import DataModelsAPI
from cognite.client._api.data_modeling.graphql import DataModelingGraphQLAPI
from cognite.client._api.data_modeling.instances import InstancesAPI
from cognite.client._api.data_modeling.spaces import SpacesAPI
from cognite.client._api.data_modeling.statistics import StatisticsAPI
from cognite.client._api.data_modeling.views import ViewsAPI
from cognite.client._api_client import APIClient
from cognite.client.utils._concurrency import ConcurrencySettings

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient
    from cognite.client.config import ClientConfig


class DataModelingAPI(APIClient):
    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: AsyncCogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.containers = ContainersAPI(config, api_version, cognite_client)
        self.data_models = DataModelsAPI(config, api_version, cognite_client)
        self.spaces = SpacesAPI(config, api_version, cognite_client)
        self.views = ViewsAPI(config, api_version, cognite_client)
        self.instances = InstancesAPI(config, api_version, cognite_client)
        self.graphql = DataModelingGraphQLAPI(config, api_version, cognite_client)
        self.statistics = StatisticsAPI(config, api_version, cognite_client)

    def _get_semaphore(self, operation: Literal["read", "write", "delete"]) -> asyncio.BoundedSemaphore:
        factory = ConcurrencySettings._semaphore_factory("data_modeling")
        return factory(operation, self._cognite_client.config.project)
