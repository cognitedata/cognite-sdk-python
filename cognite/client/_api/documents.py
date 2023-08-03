from __future__ import annotations

from typing import Literal, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DOCUMENT_LIST_LIMIT_DEFAULT
from cognite.client.data_classes.documents import Document, DocumentHighlightList, DocumentList
from cognite.client.data_classes.filters import Filter


class DocumentsAPI(APIClient):
    _RESOURCE_PATH = "/documents"

    def aggregate(self) -> None:
        ...

    def retrieve_content(self, id: int) -> str:
        """`Retrieve document content <https://developer.cognite.com/api#tag/Documents/operation/documentsContent>`_

        Returns extracted textual information for the given document.

        The documents pipeline extracts up to 1MiB of textual information from each processed document.
        The search and list endpoints truncate the textual content of each document,
        in order to reduce the size of the returned payload. If you want the whole text for a document,
        you can use this endpoint.


        Args:
            id: The server-generated ID for the document you want to retrieve the content of.

        Returns:
            str: The content of the document.

        """

        response = self._do_request("GET", f"{self._RESOURCE_PATH}/{id}/content", accept="text/plain")
        if not self._status_ok(response.status_code):
            self._raise_api_error(response, payload={})
        return response.text

    @overload
    def search(
        self,
        query: str,
        highlight: Literal[False] = False,
        filter: Filter | dict | None = None,
        sort: str | None = None,
        limit: int = DOCUMENT_LIST_LIMIT_DEFAULT,
    ) -> DocumentList:
        ...

    @overload
    def search(
        self,
        query: str,
        highlight: Literal[True],
        filter: Filter | dict | None = None,
        sort: str | None = None,
        limit: int = DOCUMENT_LIST_LIMIT_DEFAULT,
    ) -> DocumentHighlightList:
        ...

    def search(
        self,
        query: str,
        highlight: bool = False,
        filter: Filter | dict | None = None,
        sort: str | None = None,
        limit: int = DOCUMENT_LIST_LIMIT_DEFAULT,
    ) -> DocumentList | DocumentHighlightList:
        """Search documents <https://developer.cognite.com/api#tag/Documents/operation/documentsSearch>`_

        This endpoint lets you search for documents by using advanced filters and free text queries.
        Free text queries are matched against the documents' filenames and contents. For more information, see
        endpoint documentation referenced above.

        Args:
            query (str) : The free text search query.
            highlight: Whether or not matches in search results should be highlighted.
            filter (Filter | dict | None): The filter to narrow down the documents to search.
            sort: The property to sort by.
            limit: Maximum number of items. When using highlights, the maximum value is reduced to 20. Defaults to 100.

        Returns:
            DocumentList | DocumentHighlightList: List of search results. If highlight is True, a DocumentHighlightList
                                                  is returned, otherwise a DocumentList is returned.
        """
        ...
        # self._search()

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
