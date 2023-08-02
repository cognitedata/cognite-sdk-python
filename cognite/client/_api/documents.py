from __future__ import annotations

from cognite.client._api_client import APIClient
from cognite.client._constants import DOCUMENT_LIST_LIMIT_DEFAULT
from cognite.client.data_classes.documents import DocumentList
from cognite.client.data_classes.filters import Filter


class DocumentsAPI(APIClient):
    _RESOURCE_PATH = "/documents"

    def aggregate(self) -> None:
        ...

    def retrieve_content(self, id: int) -> str:
        ...

    def search(self) -> None:
        ...

    def list(self, filter: Filter | dict | None = None, limit: int = DOCUMENT_LIST_LIMIT_DEFAULT) -> DocumentList:
        ...
