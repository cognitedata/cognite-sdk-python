from __future__ import annotations

from typing import Any, Literal, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DOCUMENT_LIST_LIMIT_DEFAULT
from cognite.client.data_classes._base import EnumProperty
from cognite.client.data_classes.documents import (
    Document,
    DocumentHighlightList,
    DocumentList,
    DocumentProperty,
    DocumentSort,
    DocumentUniqueResultList,
    SortablePropertyLike,
    SourceFileProperty,
)
from cognite.client.data_classes.filters import Filter


class DocumentsAPI(APIClient):
    _RESOURCE_PATH = "/documents"

    @overload
    def _documents_aggregate(
        self,
        aggregate: Literal["count", "cardinalityValues", "cardinalityProperties"],
        properties: list[str] | None = None,
        path: list[str] | None = None,
        query: str | None = None,
        filter: Filter | dict | None = None,
        aggregate_filter: Filter | dict | None = None,
        limit: int | None = None,
    ) -> int:
        ...

    @overload
    def _documents_aggregate(
        self,
        aggregate: Literal["uniqueValues", "uniqueProperties"],
        properties: list[str] | None = None,
        path: list[str] | None = None,
        query: str | None = None,
        filter: Filter | dict | None = None,
        aggregate_filter: Filter | dict | None = None,
        limit: int | None = None,
    ) -> DocumentUniqueResultList:
        ...

    def _documents_aggregate(
        self,
        aggregate: Literal["count", "cardinalityValues", "cardinalityProperties", "uniqueValues", "uniqueProperties"],
        properties: list[str] | None = None,
        path: list[str] | None = None,
        query: str | None = None,
        filter: Filter | dict | None = None,
        aggregate_filter: Filter | dict | None = None,
        limit: int | None = None,
    ) -> int | DocumentUniqueResultList:
        body: dict[str, Any] = {
            "aggregate": aggregate,
        }
        if properties is not None:
            body["properties"] = [{"property": properties}]
        if path is not None:
            body["path"] = path
        if query is not None:
            body["search"] = {"query": query}
        if filter is not None:
            body["filter"] = filter.dump() if isinstance(filter, Filter) else filter
        if aggregate_filter is not None:
            body["aggregateFilter"] = (
                aggregate_filter.dump() if isinstance(aggregate_filter, Filter) else aggregate_filter
            )
        if limit is not None:
            body["limit"] = limit

        res = self._post(url_path=f"{self._RESOURCE_PATH}/aggregate", json=body)
        json_items = res.json()["items"]
        if aggregate in {"count", "cardinalityValues", "cardinalityProperties"}:
            return json_items[0]["count"]
        elif aggregate in {"uniqueValues", "uniqueProperties"}:
            return DocumentUniqueResultList._load(json_items, cognite_client=self._cognite_client)
        else:
            raise ValueError(f"Unknown aggregate: {aggregate}")

    @classmethod
    def _to_property_list(cls, property: EnumProperty | list[str] | str) -> list[str]:
        if isinstance(property, EnumProperty):
            return property.as_reference()
        elif isinstance(property, str):
            return [property]
        elif isinstance(property, list):
            return property
        else:
            raise ValueError(f"Unknown property format: {property}")

    def aggregate_count(self, query: str | None = None, filter: Filter | dict | None = None) -> int:
        """`Count of documents matching the specified filters and search.<https://developer.cognite.com/api#tag/Documents/operation/documentsAggregate>`_

        Args:
            query (str | None): The free text search query, for details see the documentation referenced above.
            filter (Filter | dict | None): The filter to narrow down the documents to count.

        Returns:
            int: The number of documents matching the specified filters and search.
        """
        return self._documents_aggregate("count", filter=filter, query=query)

    def aggregate_cardinality(
        self,
        property: DocumentProperty | SourceFileProperty | list[str] | str,
        query: str | None = None,
        filter: Filter | dict | None = None,
        aggregate_filter: Filter | dict | None = None,
    ) -> int:
        """`Find approximate number of unique properties.<https://developer.cognite.com/api#tag/Documents/operation/documentsAggregate>`_

        Args:
            property (DocumentProperty | list[str] | str): The property to count the cardinality of.
            query (str | None): The free text search query, for details see the documentation referenced above.
            filter (Filter | dict | None): The filter to narrow down the documents to count cardinality.
            aggregate_filter (Filter | dict | None): The filter to apply to aggregations.

        Returns:
            int: The number of documents matching the specified filters and search.
        """
        property = self._to_property_list(property)

        if property == ["sourceFile", "metadata"]:
            return self._documents_aggregate(
                "cardinalityProperties", path=property, query=query, filter=filter, aggregate_filter=aggregate_filter
            )
        else:
            return self._documents_aggregate(
                "cardinalityValues",
                properties=property,
                query=query,
                filter=filter,
                aggregate_filter=aggregate_filter,
            )

    def aggregate_unique(
        self,
        property: DocumentProperty | SourceFileProperty | list[str] | str,
        query: str | None = None,
        filter: Filter | dict | None = None,
        aggregate_filter: Filter | dict | None = None,
        limit: int = DOCUMENT_LIST_LIMIT_DEFAULT,
    ) -> DocumentUniqueResultList:
        """`Find approximate number of unique properties..<https://developer.cognite.com/api#tag/Documents/operation/documentsAggregate>`_

        Args:
            property (list[str]): The property to group by.
            query (str | None): The free text search query, for details see the documentation referenced above.
            filter (Filter | dict | None): The filter to narrow down the documents to count cardinality.
            aggregate_filter (Filter | dict | None): The filter to apply to aggregations.
            limit (int): Maximum number of items. Defaults to 100.

        Returns:
            DocumentUniqueResultList: List of unique values of documents matching the specified filters and search.
        """
        property = self._to_property_list(property)
        aggregate: Literal["uniqueValues", "uniqueProperties"] = (
            "uniqueProperties" if property == ["sourceFile", "metadata"] else "uniqueValues"
        )
        return self._documents_aggregate(
            aggregate=aggregate,
            properties=property,
            query=query,
            filter=filter,
            aggregate_filter=aggregate_filter,
            limit=limit,
        )

    def retrieve_content(self, id: int) -> str:
        """`Retrieve document content <https://developer.cognite.com/api#tag/Documents/operation/documentsContent>`_

        Returns extracted textual information for the given document.

        The document pipeline extracts up to 1MiB of textual information from each processed document.
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
        sort: DocumentSort | str | list[str] | tuple[SortablePropertyLike, Literal["asc", "desc"]] | None = None,
        limit: int = DOCUMENT_LIST_LIMIT_DEFAULT,
    ) -> DocumentList:
        ...

    @overload
    def search(
        self,
        query: str,
        highlight: Literal[True],
        filter: Filter | dict | None = None,
        sort: DocumentSort | str | list[str] | tuple[SortablePropertyLike, Literal["asc", "desc"]] | None = None,
        limit: int = DOCUMENT_LIST_LIMIT_DEFAULT,
    ) -> DocumentHighlightList:
        ...

    def search(
        self,
        query: str,
        highlight: bool = False,
        filter: Filter | dict | None = None,
        sort: DocumentSort | SortablePropertyLike | tuple[SortablePropertyLike, Literal["asc", "desc"]] | None = None,
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
            sort (DocumentSort | str | list[str] | tuple[SortablePropertyLike, Literal["asc", "desc"]] | None):
                The property to sort by. The default order is ascending.
            limit: Maximum number of items. When using highlights, the maximum value is reduced to 20. Defaults to 100.

        Returns:
            DocumentList | DocumentHighlightList: List of search results. If highlight is True, a DocumentHighlightList
                                                  is returned, otherwise a DocumentList is returned.
        """
        results = []
        next_cursor = None
        body: dict[str, str | int | bool | dict | list] = {
            "search": {"query": query},
        }
        if filter:
            body["filter"] = filter.dump() if isinstance(filter, Filter) else filter
        if sort:
            body["sort"] = [DocumentSort.load(sort).dump()]
        if limit:
            body["limit"] = limit
        if highlight:
            body["highlight"] = highlight
        while True:
            if next_cursor:
                body["cursor"] = next_cursor

            response = self._post(f"{self._RESOURCE_PATH}/search", json=body)
            if not self._status_ok(response.status_code):
                self._raise_api_error(response, payload={})
            json_content = response.json()
            results.extend(json_content["items"])
            next_cursor = json_content.get("nextCursor")
            if not next_cursor:
                break

        if highlight:
            return DocumentHighlightList._load(
                ({"highlight": item["highlight"], "document": item["item"]} for item in json_content["items"]),
                cognite_client=self._cognite_client,
            )
        return DocumentList._load((item["item"] for item in results), cognite_client=self._cognite_client)

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
