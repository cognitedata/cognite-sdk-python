from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from cognite.client._api.spaces import SpacesAPI
from cognite.client._api_client import APIClient

if TYPE_CHECKING:
    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig


class DataModelingAPI(APIClient):
    def __init__(self, config: ClientConfig, api_version: Optional[str], cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.spaces = SpacesAPI(config, api_version, cognite_client)
