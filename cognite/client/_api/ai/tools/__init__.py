from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client._api.ai.tools.documents import DocumentsAPI
from cognite.client._api_client import APIClient

if TYPE_CHECKING:
    from cognite.client import ClientConfig, CogniteClient


class ToolsAPI(APIClient):
    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.documents = DocumentsAPI(config, api_version, cognite_client)
