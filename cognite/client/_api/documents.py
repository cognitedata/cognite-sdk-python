from __future__ import annotations

from cognite.client._api_client import APIClient
from cognite.client._constants import DOCUMENT_LIST_LIMIT_DEFAULT
from cognite.client.data_classes.documents import Document, DocumentList
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
        """`List documents <https://developer.cognite.com/api#tag/Documents/operation/documentsList>`_

         You can use filters to narrow down the list. Unlike the search method, list does not restrict the number
         of documents to return, meaning that setting the limit to -1 will return all the documents in your
         project.

        Args:
            filter(filter: Filter | dict | None): The filter to narrow down the documents to return.
            limit (int): Maximum number of documents to return. Defaults to 100. Set to -1 to return all documents.

        Returns:
            DocumentList: List of documents
        """
        return self._list(
            list_cls=DocumentList,
            resource_cls=Document,
            method="POST",
            limit=limit,
            filter=filter.dump() if isinstance(filter, Filter) else filter,
        )
