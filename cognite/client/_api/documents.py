from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import IO, TYPE_CHECKING, Any, BinaryIO, Literal, cast, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import filters
from cognite.client.data_classes.aggregations import AggregationFilter, UniqueResultList
from cognite.client.data_classes.documents import (
    Document,
    DocumentHighlightList,
    DocumentList,
    DocumentProperty,
    DocumentSort,
    SortableProperty,
    SourceFileProperty,
    TemporaryLink,
)
from cognite.client.data_classes.filters import _BASIC_FILTERS, Filter, _validate_filter

if TYPE_CHECKING:
    from cognite.client import ClientConfig, CogniteClient

_FILTERS_SUPPORTED: frozenset[type[Filter]] = _BASIC_FILTERS.union(
    {filters.InAssetSubtree, filters.Search, filters.GeoJSONIntersects, filters.GeoJSONDisjoint, filters.GeoJSONWithin}
)


class DocumentPreviewAPI(APIClient):
    _RESOURCE_PATH = "/documents"

    def download_page_as_png_bytes(self, id: int, page_number: int = 1) -> bytes:
        """`Downloads an image preview for a specific page of the specified document. <https://developer.cognite.com/api#tag/Document-preview/operation/documentsPreviewImagePage>`_

        Args:
            id (int): The server-generated ID for the document you want to retrieve the preview of.
            page_number (int): Page number to preview. Starting at 1 for first page.

        Returns:
            bytes: The png preview of the document.

        Examples:

            Download image preview of page 5 of file with id 123:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> content = client.documents.previews.download_page_as_png_bytes(id=123, page_number=5)

            Download an image preview and display using IPython.display.Image (for example in a Jupyter Notebook):

                >>> from IPython.display import Image
                >>> binary_png = client.documents.previews.download_page_as_png_bytes(id=123, page_number=5)
                >>> Image(binary_png)
        """
        res = self._do_request(
            "GET", f"{self._RESOURCE_PATH}/{id}/preview/image/pages/{page_number}", accept="image/png"
        )
        return res.content

    def download_page_as_png(
        self, path: Path | str | IO, id: int, page_number: int = 1, overwrite: bool = False
    ) -> None:
        """`Downloads an image preview for a specific page of the specified document. <https://developer.cognite.com/api#tag/Document-preview/operation/documentsPreviewImagePage>`_

        Args:
            path (Path | str | IO): The path to save the png preview of the document. If the path is a directory, the file name will be '[id]_page[page_number].png'.
            id (int): The server-generated ID for the document you want to retrieve the preview of.
            page_number (int): Page number to preview. Starting at 1 for first page.
            overwrite (bool): Whether to overwrite existing file at the given path. Defaults to False.

        Examples:

            Download Image preview of page 5 of file with id 123 to folder "previews":

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.documents.previews.download_page_as_png("previews", id=123, page_number=5)
        """
        if isinstance(path, IO):
            content = self.download_page_as_png_bytes(id)
            path.write(content)
            return

        if (path := Path(path)).is_dir():
            path /= f"{id}_page{page_number}.png"
        elif path.suffix != ".png":
            raise ValueError("Path must be a directory or end with .png")
        if not overwrite and path.exists():
            raise FileExistsError(f"File {path} already exists. Use overwrite=True to overwrite existing file.")
        content = self.download_page_as_png_bytes(id, page_number)
        path.write_bytes(content)

    def download_document_as_pdf_bytes(self, id: int) -> bytes:
        """`Downloads a pdf preview of the specified document. <https://developer.cognite.com/api#tag/Document-preview/operation/documentsPreviewPdf>`_

        Previews will be rendered if necessary during the request. Be prepared for the request to take a few seconds to complete.

        Args:
            id (int): The server-generated ID for the document you want to retrieve the preview of.

        Returns:
            bytes: The pdf preview of the document.

        Examples:

            Download PDF preview of file with id 123:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> content = client.documents.previews.download_document_as_pdf_bytes(id=123)
        """
        res = self._do_request("GET", f"{self._RESOURCE_PATH}/{id}/preview/pdf", accept="application/pdf")
        return res.content

    def download_document_as_pdf(self, path: Path | str | IO, id: int, overwrite: bool = False) -> None:
        """`Downloads a pdf preview of the specified document. <https://developer.cognite.com/api#tag/Document-preview/operation/documentsPreviewPdf>`_

        Previews will be rendered if necessary during the request. Be prepared for the request to take a few seconds to complete.

        Args:
            path (Path | str | IO): The path to save the pdf preview of the document. If the path is a directory, the file name will be '[id].pdf'.
            id (int): The server-generated ID for the document you want to retrieve the preview of.
            overwrite (bool): Whether to overwrite existing file at the given path. Defaults to False.

        Examples:

            Download PDF preview of file with id 123 to folder "previews":

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.documents.previews.download_document_as_pdf("previews", id=123)
        """
        if isinstance(path, IO):
            content = self.download_document_as_pdf_bytes(id)
            path.write(content)
            return

        if (path := Path(path)).is_dir():
            path /= f"{id}.pdf"
        elif path.suffix != ".pdf":
            raise ValueError("Path must be a directory or end with .pdf")
        if not overwrite and path.exists():
            raise FileExistsError(f"File {path} already exists. Use overwrite=True to overwrite existing file.")
        content = self.download_document_as_pdf_bytes(id)
        path.write_bytes(content)

    def retrieve_pdf_link(self, id: int) -> TemporaryLink:
        """`Retrieve a Temporary link to download pdf preview <https://developer.cognite.com/api#tag/Document-preview/operation/documentsPreviewPdfTemporaryLink>`_

        Args:
            id (int): The server-generated ID for the document you want to retrieve the preview of.

        Returns:
            TemporaryLink: A temporary link to download the pdf preview.

        Examples:

            Retrieve the PDF preview download link for document with id 123:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> link = client.documents.previews.retrieve_pdf_link(id=123)
        """
        res = self._get(f"{self._RESOURCE_PATH}/{id}/preview/pdf/temporarylink")
        return TemporaryLink.load(res.json())


class DocumentsAPI(APIClient):
    _RESOURCE_PATH = "/documents"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.previews = DocumentPreviewAPI(config, api_version, cognite_client)

    @overload
    def __call__(
        self,
        chunk_size: int,
        filter: Filter | dict[str, Any] | None = None,
        sort: DocumentSort | SortableProperty | tuple[SortableProperty, Literal["asc", "desc"]] | None = None,
        limit: int | None = None,
        partitions: int | None = None,
    ) -> Iterator[DocumentList]: ...

    @overload
    def __call__(
        self,
        chunk_size: Literal[None] = None,
        filter: Filter | dict[str, Any] | None = None,
        sort: DocumentSort | SortableProperty | tuple[SortableProperty, Literal["asc", "desc"]] | None = None,
        limit: int | None = None,
        partitions: int | None = None,
    ) -> Iterator[DocumentList]: ...

    def __call__(
        self,
        chunk_size: int | None = None,
        filter: Filter | dict[str, Any] | None = None,
        sort: DocumentSort | SortableProperty | tuple[SortableProperty, Literal["asc", "desc"]] | None = None,
        limit: int | None = None,
        partitions: int | None = None,
    ) -> Iterator[Document] | Iterator[DocumentList]:
        """Iterate over documents

        Fetches documents as they are iterated over, so you keep a limited number of documents in memory.

        Args:
            chunk_size (int | None): Number of documents to return in each chunk. Defaults to yielding one document at a time.
            filter (Filter | dict[str, Any] | None): The filter to narrow down the documents to return.
            sort (DocumentSort | SortableProperty | tuple[SortableProperty, Literal['asc', 'desc']] | None): The property to sort by. The default order is ascending.
            limit (int | None): Maximum number of documents to return. Default to return all items.
            partitions (int | None): Retrieve documents in parallel using this number of workers. Also requires `limit=None` to be passed. To prevent unexpected problems and maximize read throughput, API documentation recommends at most use 10 partitions. When using more than 10 partitions, actual throughout decreases. In future releases of the APIs, CDF may reject requests with more than 10 partitions.

        Returns:
            Iterator[Document] | Iterator[DocumentList]: yields Documents one by one if chunk_size is not specified, else DocumentList objects.
        """
        self._validate_filter(filter)
        return self._list_generator(
            list_cls=DocumentList,
            resource_cls=Document,
            sort=[DocumentSort.load(sort).dump()] if sort else None,
            method="POST",
            chunk_size=chunk_size,
            filter=filter.dump() if isinstance(filter, Filter) else filter,
            limit=limit,
            partitions=partitions,
        )

    def __iter__(self) -> Iterator[Document]:
        """Iterate over documents

        Fetches documents as they are iterated over, so you keep a limited number of documents in memory.

        Returns:
            Iterator[Document]: yields documents one by one.
        """
        return cast(Iterator[Document], self())

    def aggregate_count(self, query: str | None = None, filter: Filter | dict[str, Any] | None = None) -> int:
        """`Count of documents matching the specified filters and search. <https://developer.cognite.com/api#tag/Documents/operation/documentsAggregate>`_

        Args:
            query (str | None): The free text search query, for details see the documentation referenced above.
            filter (Filter | dict[str, Any] | None): The filter to narrow down the documents to count.

        Returns:
            int: The number of documents matching the specified filters and search.

        Examples:

            Count the number of documents in your CDF project:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> count = client.documents.aggregate_count()

            Count the number of PDF documents in your CDF project:

                >>> from cognite.client.data_classes import filters
                >>> from cognite.client.data_classes.documents import DocumentProperty
                >>> is_pdf = filters.Equals(DocumentProperty.mime_type, "application/pdf")
                >>> pdf_count = client.documents.aggregate_count(filter=is_pdf)

            Count the number of documents with a related asset in a subtree rooted at any of
            the specified external IDs, e.g. 'Plant_1' and 'Plant_2':

                >>> client.documents.aggregate_count(
                ...     filter=filters.InAssetSubtree(
                ...         property=DocumentProperty.asset_external_ids,
                ...         values=['Plant_1', 'Plant_2'],
                ...     )
                ... )
        """
        self._validate_filter(filter)
        return self._advanced_aggregate(
            "count", filter=filter.dump() if isinstance(filter, Filter) else filter, query=query
        )

    def aggregate_cardinality_values(
        self,
        property: DocumentProperty | SourceFileProperty | list[str] | str,
        query: str | None = None,
        filter: Filter | dict[str, Any] | None = None,
        aggregate_filter: AggregationFilter | dict[str, Any] | None = None,
    ) -> int:
        """`Find approximate property count for documents. <https://developer.cognite.com/api#tag/Documents/operation/documentsAggregate>`_

        Args:
            property (DocumentProperty | SourceFileProperty | list[str] | str): The property to count the cardinality of.
            query (str | None): The free text search query, for details see the documentation referenced above.
            filter (Filter | dict[str, Any] | None): The filter to narrow down the documents to count cardinality.
            aggregate_filter (AggregationFilter | dict[str, Any] | None): The filter to apply to the resulting buckets.

        Returns:
            int: The number of documents matching the specified filters and search.

        Examples:

            Count the number of types of documents in your CDF project:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.documents import DocumentProperty
                >>> client = CogniteClient()
                >>> count = client.documents.aggregate_cardinality_values(DocumentProperty.type)

            Count the number of authors of plain/text documents in your CDF project:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import filters
                >>> from cognite.client.data_classes.documents import DocumentProperty
                >>> client = CogniteClient()
                >>> is_plain_text = filters.Equals(DocumentProperty.mime_type, "text/plain")
                >>> plain_text_author_count = client.documents.aggregate_cardinality_values(DocumentProperty.author, filter=is_plain_text)

            Count the number of types of documents in your CDF project but exclude documents that start with "text":

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.documents import DocumentProperty
                >>> from cognite.client.data_classes import aggregations
                >>> client = CogniteClient()
                >>> agg = aggregations
                >>> is_not_text = agg.Not(agg.Prefix("text"))
                >>> type_count_excluded_text = client.documents.aggregate_cardinality_values(DocumentProperty.type, aggregate_filter=is_not_text)
        """
        self._validate_filter(filter)

        return self._advanced_aggregate(
            "cardinalityValues",
            properties=property,
            query=query,
            filter=filter.dump() if isinstance(filter, Filter) else filter,
            aggregate_filter=aggregate_filter,
        )

    def aggregate_cardinality_properties(
        self,
        path: DocumentProperty | SourceFileProperty | list[str] | str,
        query: str | None = None,
        filter: Filter | dict[str, Any] | None = None,
        aggregate_filter: AggregationFilter | dict[str, Any] | None = None,
    ) -> int:
        """`Find approximate paths count for documents.  <https://developer.cognite.com/api#tag/Documents/operation/documentsAggregate>`_

        Args:
            path (DocumentProperty | SourceFileProperty | list[str] | str): The scope in every document to aggregate properties. The only value allowed now is ["metadata"]. It means to aggregate only metadata properties (aka keys).
            query (str | None): The free text search query, for details see the documentation referenced above.
            filter (Filter | dict[str, Any] | None): The filter to narrow down the documents to count cardinality.
            aggregate_filter (AggregationFilter | dict[str, Any] | None): The filter to apply to the resulting buckets.

        Returns:
            int: The number of documents matching the specified filters and search.

        Examples:

            Count the number metadata keys for documents in your CDF project:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.documents import SourceFileProperty
                >>> client = CogniteClient()
                >>> count = client.documents.aggregate_cardinality_properties(SourceFileProperty.metadata)
        """
        self._validate_filter(filter)

        return self._advanced_aggregate(
            "cardinalityProperties",
            path=path,
            query=query,
            filter=filter.dump() if isinstance(filter, Filter) else filter,
            aggregate_filter=aggregate_filter,
        )

    def aggregate_unique_values(
        self,
        property: DocumentProperty | SourceFileProperty | list[str] | str,
        query: str | None = None,
        filter: Filter | dict[str, Any] | None = None,
        aggregate_filter: AggregationFilter | dict[str, Any] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
    ) -> UniqueResultList:
        """`Get unique properties with counts for documents. <https://developer.cognite.com/api#tag/Documents/operation/documentsAggregate>`_

        Args:
            property (DocumentProperty | SourceFileProperty | list[str] | str): The property to group by.
            query (str | None): The free text search query, for details see the documentation referenced above.
            filter (Filter | dict[str, Any] | None): The filter to narrow down the documents to count cardinality.
            aggregate_filter (AggregationFilter | dict[str, Any] | None): The filter to apply to the resulting buckets.
            limit (int): Maximum number of items. Defaults to 25.

        Returns:
            UniqueResultList: List of unique values of documents matching the specified filters and search.

        Examples:

            Get the unique types with count of documents in your CDF project:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.documents import DocumentProperty
                >>> client = CogniteClient()
                >>> result = client.documents.aggregate_unique_values(DocumentProperty.mime_type)
                >>> unique_types = result.unique

            Get the different languages with count for documents with external id prefix "abc":

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import filters
                >>> from cognite.client.data_classes.documents import DocumentProperty
                >>> client = CogniteClient()
                >>> is_abc = filters.Prefix(DocumentProperty.external_id, "abc")
                >>> result = client.documents.aggregate_unique_values(DocumentProperty.language, filter=is_abc)
                >>> unique_languages = result.unique

            Get the unique mime types with count of documents, but exclude mime types that start with text:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.documents import DocumentProperty
                >>> from cognite.client.data_classes import aggregations
                >>> client = CogniteClient()
                >>> agg = aggregations
                >>> is_not_text = agg.Not(agg.Prefix("text"))
                >>> result = client.documents.aggregate_unique_values(DocumentProperty.mime_type, aggregate_filter=is_not_text)
                >>> unique_mime_types = result.unique
        """
        self._validate_filter(filter)
        return self._advanced_aggregate(
            aggregate="uniqueValues",
            properties=property,
            query=query,
            filter=filter.dump() if isinstance(filter, Filter) else filter,
            aggregate_filter=aggregate_filter,
            limit=limit,
        )

    def aggregate_unique_properties(
        self,
        path: DocumentProperty | SourceFileProperty | list[str] | str,
        query: str | None = None,
        filter: Filter | dict[str, Any] | None = None,
        aggregate_filter: AggregationFilter | dict[str, Any] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
    ) -> UniqueResultList:
        """`Get unique paths with counts for documents. <https://developer.cognite.com/api#tag/Documents/operation/documentsAggregate>`_

        Args:
            path (DocumentProperty | SourceFileProperty | list[str] | str): The scope in every document to aggregate properties. The only value allowed now is ["metadata"]. It means to aggregate only metadata properties (aka keys).
            query (str | None): The free text search query, for details see the documentation referenced above.
            filter (Filter | dict[str, Any] | None): The filter to narrow down the documents to count cardinality.
            aggregate_filter (AggregationFilter | dict[str, Any] | None): The filter to apply to the resulting buckets.
            limit (int): Maximum number of items. Defaults to 25.

        Returns:
            UniqueResultList: List of unique values of documents matching the specified filters and search.

        Examples:

            Get the unique metadata keys with count of documents in your CDF project:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.documents import SourceFileProperty
                >>> client = CogniteClient()
                >>> result = client.documents.aggregate_unique_values(SourceFileProperty.metadata)
        """
        self._validate_filter(filter)

        return self._advanced_aggregate(
            aggregate="uniqueProperties",
            # There is a bug/inconsistency in the API where the path parameter is called properties for documents.
            # This has been reported to the API team, and will be fixed in the future.
            properties=path,
            query=query,
            filter=filter.dump() if isinstance(filter, Filter) else filter,
            aggregate_filter=aggregate_filter,
            limit=limit,
        )

    def retrieve_content(self, id: int) -> bytes:
        """`Retrieve document content <https://developer.cognite.com/api#tag/Documents/operation/documentsContent>`_

        Returns extracted textual information for the given document.

        The document pipeline extracts up to 1MiB of textual information from each processed document.
        The search and list endpoints truncate the textual content of each document,
        in order to reduce the size of the returned payload. If you want the whole text for a document,
        you can use this endpoint.


        Args:
            id (int): The server-generated ID for the document you want to retrieve the content of.

        Returns:
            bytes: The content of the document.

        Examples:

            Retrieve the content of a document with id 123:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> content = client.documents.retrieve_content(id=123)
        """
        response = self._do_request("GET", f"{self._RESOURCE_PATH}/{id}/content", accept="text/plain")
        return response.content

    def retrieve_content_buffer(self, id: int, buffer: BinaryIO) -> None:
        """`Retrieve document content into buffer <https://developer.cognite.com/api#tag/Documents/operation/documentsContent>`_

        Returns extracted textual information for the given document.

        The document pipeline extracts up to 1MiB of textual information from each processed document.
        The search and list endpoints truncate the textual content of each document,
        in order to reduce the size of the returned payload. If you want the whole text for a document,
        you can use this endpoint.


        Args:
            id (int): The server-generated ID for the document you want to retrieve the content of.
            buffer (BinaryIO): The document content is streamed directly into the buffer. This is useful for retrieving large documents.

        Examples:

            Retrieve the content of a document with id 123 into local file "my_text.txt":

                >>> from cognite.client import CogniteClient
                >>> from pathlib import Path
                >>> client = CogniteClient()
                >>> with Path("my_file.txt").open("wb") as buffer:
                ...     client.documents.retrieve_content_buffer(id=123, buffer=buffer)
        """
        with self._do_request(
            "GET", f"{self._RESOURCE_PATH}/{id}/content", stream=True, accept="text/plain"
        ) as response:
            for chunk in response.iter_content(chunk_size=2**21):
                if chunk:  # filter out keep-alive new chunks
                    buffer.write(chunk)

    @overload
    def search(
        self,
        query: str,
        highlight: Literal[False] = False,
        filter: Filter | dict[str, Any] | None = None,
        sort: DocumentSort | str | list[str] | tuple[SortableProperty, Literal["asc", "desc"]] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
    ) -> DocumentList: ...

    @overload
    def search(
        self,
        query: str,
        highlight: Literal[True],
        filter: Filter | dict[str, Any] | None = None,
        sort: DocumentSort | str | list[str] | tuple[SortableProperty, Literal["asc", "desc"]] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
    ) -> DocumentHighlightList: ...

    def search(
        self,
        query: str,
        highlight: bool = False,
        filter: Filter | dict[str, Any] | None = None,
        sort: DocumentSort | SortableProperty | tuple[SortableProperty, Literal["asc", "desc"]] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
    ) -> DocumentList | DocumentHighlightList:
        """`Search documents <https://developer.cognite.com/api#tag/Documents/operation/documentsSearch>`_

        This endpoint lets you search for documents by using advanced filters and free text queries.
        Free text queries are matched against the documents' filenames and contents. For more information, see
        endpoint documentation referenced above.

        Args:
            query (str): The free text search query.
            highlight (bool): Whether or not matches in search results should be highlighted.
            filter (Filter | dict[str, Any] | None): The filter to narrow down the documents to search.
            sort (DocumentSort | SortableProperty | tuple[SortableProperty, Literal['asc', 'desc']] | None): The property to sort by. The default order is ascending.
            limit (int): Maximum number of items to return. When using highlights, the maximum value is reduced to 20. Defaults to 25.

        Returns:
            DocumentList | DocumentHighlightList: List of search results. If highlight is True, a DocumentHighlightList is returned, otherwise a DocumentList is returned.

        Examples:

            Search for text "pump 123" in PDF documents in your CDF project:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import filters
                >>> from cognite.client.data_classes.documents import DocumentProperty
                >>> client = CogniteClient()
                >>> is_pdf = filters.Equals(DocumentProperty.mime_type, "application/pdf")
                >>> documents = client.documents.search("pump 123", filter=is_pdf)

            Find all documents with exact text 'CPLEX Error 1217: No Solution exists.'
            in plain text files created the last week in your CDF project and highlight the matches:

                >>> from datetime import datetime, timedelta
                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import filters
                >>> from cognite.client.data_classes.documents import DocumentProperty
                >>> from cognite.client.utils import timestamp_to_ms
                >>> client = CogniteClient()
                >>> is_plain_text = filters.Equals(DocumentProperty.mime_type, "text/plain")
                >>> last_week = filters.Range(DocumentProperty.created_time,
                ...     gt=timestamp_to_ms(datetime.now() - timedelta(days=7)))
                >>> documents = client.documents.search('"CPLEX Error 1217: No Solution exists."',
                ...     highlight=True,
                ...     filter=filters.And(is_plain_text, last_week))
        """
        self._validate_filter(filter)
        results = []
        next_cursor = None
        body: dict[str, str | int | bool | dict | list] = {"search": {"query": query}}
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
            json_content = response.json()
            results.extend(json_content["items"])
            if not (next_cursor := json_content.get("nextCursor")):
                break

        if highlight:
            return DocumentHighlightList._load(
                ({"highlight": item["highlight"], "document": item["item"]} for item in json_content["items"]),
                cognite_client=self._cognite_client,
            )
        return DocumentList._load((item["item"] for item in results), cognite_client=self._cognite_client)

    def list(
        self,
        filter: Filter | dict[str, Any] | None = None,
        sort: DocumentSort | SortableProperty | tuple[SortableProperty, Literal["asc", "desc"]] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> DocumentList:
        """`List documents <https://developer.cognite.com/api#tag/Documents/operation/documentsList>`_

        You can use filters to narrow down the list. Unlike the search method, list does not restrict the number
        of documents to return, meaning that setting the limit to -1 will return all the documents in your
        project.

        Args:
            filter (Filter | dict[str, Any] | None): Filter | dict[str, Any] | None): The filter to narrow down the documents to return.
            sort (DocumentSort | SortableProperty | tuple[SortableProperty, Literal['asc', 'desc']] | None): The property to sort by. The default order is ascending.
            limit (int | None): Maximum number of documents to return. Defaults to 25. Set to None or -1 to return all documents.

        Returns:
            DocumentList: List of documents

        Examples:

            List all PDF documents in your CDF project:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import filters
                >>> from cognite.client.data_classes.documents import DocumentProperty
                >>> client = CogniteClient()
                >>> is_pdf = filters.Equals(DocumentProperty.mime_type, "application/pdf")
                >>> pdf_documents = client.documents.list(filter=is_pdf)

            Iterate over all documents in your CDF project:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.documents import DocumentProperty
                >>> client = CogniteClient()
                >>> for document in client.documents:
                ...    print(document.name)

            List all documents in your CDF project sorted by mime/type in descending order:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.documents import SortableDocumentProperty
                >>> client = CogniteClient()
                >>> documents = client.documents.list(sort=(SortableDocumentProperty.mime_type, "desc"))

        """
        self._validate_filter(filter)
        return self._list(
            list_cls=DocumentList,
            resource_cls=Document,
            method="POST",
            limit=limit,
            filter=filter.dump() if isinstance(filter, Filter) else filter,
            sort=[DocumentSort.load(sort).dump()] if sort else None,
        )

    def _validate_filter(self, filter: Filter | dict[str, Any] | None) -> None:
        _validate_filter(filter, _FILTERS_SUPPORTED, type(self).__name__)
