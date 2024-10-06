from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client._api.postgres_gateway.users import UsersAPI
from cognite.client._api_client import APIClient

if TYPE_CHECKING:
    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig


class PostgresGatewaysAPI(APIClient):
    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.users = UsersAPI(config, api_version, cognite_client)
