from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client._api.data_modeling.containers import ContainersAPI
from cognite.client._api.data_modeling.data_models import DataModelsAPI
from cognite.client._api.data_modeling.graphql import DataModelingGraphQLAPI
from cognite.client._api.data_modeling.instances import InstancesAPI
from cognite.client._api.data_modeling.spaces import SpacesAPI
from cognite.client._api.data_modeling.views import ViewsAPI
from cognite.client._api_client import APIClient

if TYPE_CHECKING:
    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig


class DataModelingAPI(APIClient):
    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.containers = ContainersAPI(config, api_version, cognite_client)
        self.data_models = DataModelsAPI(config, api_version, cognite_client)
        self.spaces = SpacesAPI(config, api_version, cognite_client)
        self.views = ViewsAPI(config, api_version, cognite_client)
        self.instances = InstancesAPI(config, api_version, cognite_client)
        self.graphql = DataModelingGraphQLAPI(config, api_version, cognite_client)
