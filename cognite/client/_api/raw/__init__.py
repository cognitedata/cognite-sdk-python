from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client._api.raw.databases import RawDatabasesAPI
from cognite.client._api.raw.rows import RawRowsAPI
from cognite.client._api.raw.tables import RawTablesAPI
from cognite.client._api_client import APIClient

if TYPE_CHECKING:
    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig


class RawAPI(APIClient):
    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.databases = RawDatabasesAPI(config, api_version, cognite_client)
        self.tables = RawTablesAPI(config, api_version, cognite_client)
        self.rows = RawRowsAPI(config, api_version, cognite_client)
