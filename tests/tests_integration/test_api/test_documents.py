from __future__ import annotations

import inspect
import time
from io import BytesIO
from pathlib import Path

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import aggregations as agg
from cognite.client.data_classes import filters as flt
from cognite.client.data_classes.documents import (
    DocumentProperty,
    SortableDocumentProperty,
    SourceFile,
    SourceFileProperty,
)
from cognite.client.data_classes.files import FileMetadata, FileMetadataList
from cognite.client.exceptions import CogniteAPIError
from tests.utils import dict_without

RESOURCES = Path(__file__).resolve().parent / "documents_resources"

_FILE_PREFIX = "document_api_integration"

_SYMMETRIC_DIFFERENCE_FILEMETADATA_SOURCEFILE: frozenset[str] = frozenset(
    set(inspect.signature(FileMetadata.__init__).parameters) ^ set(inspect.signature(SourceFile.__init__).parameters)
)
PLAIN_TEXT_CONTENT = """This is a test file for the document API integration test.

Lorem ipsum dolor sit amet, eum an quando noster graecis, nam everti offendit an.
Quod probo mazim cu pro, pro at pericula ullamcorper, vitae cetero vis ex.
Error deserunt eu eos, te vix labores suscipiantur, ut quot atomorum comprehensam quo.
Nam cu appareat officiis, vis et enim vide possit. Dicat bonorum ancillae est at.
Mei et possim option. An sit ipsum scaevola.
"""


@pytest.fixture(scope="session")
def text_file(cognite_client: CogniteClient) -> FileMetadata:
    external_id = f"{_FILE_PREFIX}_test_file"
    file = cognite_client.files.retrieve(external_id=external_id)
    if file is not None:
        return file
    created_file = cognite_client.files.upload_bytes(
        content=PLAIN_TEXT_CONTENT,
        name=external_id,
        external_id=external_id,
        mime_type="text/plain",
        metadata={
            "solver_type": "linear",
            "im_a_key": "t-X-t",
        },
    )
    return created_file


@pytest.fixture(scope="session")
def pdf_file(cognite_client: CogniteClient) -> FileMetadata:
    external_id = f"{_FILE_PREFIX}_test_pdf_file"
    file = cognite_client.files.retrieve(external_id=external_id)
    if file is not None:
        return file

    content = (RESOURCES / "test_pdf_file.pdf").read_bytes()
    created_file = cognite_client.files.upload_bytes(
        content=content,
        name=external_id,
        external_id=external_id,
        mime_type="application/pdf",
        metadata={"im_a_key": "p-duh-f"},
    )
    return created_file


@pytest.fixture(scope="session")
def document_list(text_file: FileMetadata, pdf_file: FileMetadata) -> FileMetadataList:
    return FileMetadataList([text_file, pdf_file])


class TestDocumentsAPI:
    def test_list(
        self,
        cognite_client: CogniteClient,
        document_list: FileMetadataList,
        text_file: FileMetadata,
    ):
        is_integration_test = flt.Prefix("externalId", _FILE_PREFIX)

        documents = cognite_client.documents.list(
            limit=5, filter=is_integration_test, sort=SortableDocumentProperty.mime_type
        )
        assert len(documents) >= len(document_list)
        assert [doc.mime_type for doc in documents] == sorted(doc.mime_type for doc in document_list)
        exclude = _SYMMETRIC_DIFFERENCE_FILEMETADATA_SOURCEFILE
        retrieved_text = documents.get(id=text_file.id)
        assert retrieved_text is not None, "Expected to retrieve the text file to be the list"
        assert dict_without(retrieved_text.source_file.dump(camel_case=False), exclude) == dict_without(
            text_file.dump(camel_case=False), exclude
        )

    def test_list_lorem_ipsum(
        self,
        cognite_client: CogniteClient,
        document_list: FileMetadataList,
        pdf_file: FileMetadata,
    ):
        is_integration_test = flt.Prefix("externalId", _FILE_PREFIX)
        is_lorem = flt.Search(DocumentProperty.content, "lorem ipsum")

        documents = cognite_client.documents.list(filter=flt.And(is_lorem, is_integration_test), limit=5)

        # Both the files in the document list have "lorem ipsum" in the content
        assert len(documents) >= len(document_list)
        retrieved_pdf = documents.get(id=pdf_file.id)
        assert retrieved_pdf is not None, "Expected to retrieve the pdf file to be the list"
        exclude = _SYMMETRIC_DIFFERENCE_FILEMETADATA_SOURCEFILE
        assert dict_without(retrieved_pdf.source_file.dump(camel_case=False), exclude) == dict_without(
            pdf_file.dump(camel_case=False), exclude
        )

    def test_retrieve_content(self, cognite_client: CogniteClient, text_file: FileMetadata):
        res = cognite_client.documents.retrieve_content(id=text_file.id)
        assert isinstance(res, bytes)
        res = res.decode("utf-8")[:-1]  # remove additional newline added by Files API
        assert res == PLAIN_TEXT_CONTENT

    def test_retrieve_content_into_buffer(self, cognite_client: CogniteClient, text_file: FileMetadata):
        buffer = BytesIO()
        res = cognite_client.documents.retrieve_content_buffer(id=text_file.id, buffer=buffer)
        assert res is None
        result = buffer.getvalue().decode("utf-8")
        result = result[:-1]  # remove additional newline added by Files API
        assert result == PLAIN_TEXT_CONTENT

    def test_search_no_filters_no_highlight(self, cognite_client: CogniteClient, text_file: FileMetadata):
        query = '"pro at pericula ullamcorper"'
        result = cognite_client.documents.search(query=query, limit=5, sort=SortableDocumentProperty.title)
        assert len(result) == 1, "Expected to retrieve exactly one document."

        actual = result[0]
        assert actual.id == text_file.id
        assert actual.source_file.name == text_file.name

    def test_search_no_filter_with_highlight(self, cognite_client: CogniteClient, text_file):
        query = '"pro at pericula ullamcorper"'

        result = cognite_client.documents.search(query=query, highlight=True, limit=5)
        assert len(result) == 1, "Expected to retrieve exactly one document."

        actual = result[0]
        assert actual.document.id == text_file.id
        assert actual.document.source_file.name == text_file.name
        assert not actual.highlight.name
        assert query[1:-1] in actual.highlight.content[0]

    def test_aggregate_count(self, cognite_client: CogniteClient, document_list: FileMetadataList):
        is_integration_test = flt.Prefix("externalId", _FILE_PREFIX)

        count = cognite_client.documents.aggregate_count(filter=is_integration_test)
        assert count >= len(document_list)

    def test_aggregate_cardinality(self, cognite_client: CogniteClient, document_list: FileMetadataList):
        is_integration_test = flt.Prefix("externalId", _FILE_PREFIX)

        count = cognite_client.documents.aggregate_cardinality_values(
            property=DocumentProperty.mime_type, filter=is_integration_test
        )
        assert count >= len({doc.mime_type for doc in document_list if doc.mime_type is not None})

    def test_aggregate_cardinality_metadata(self, cognite_client: CogniteClient, document_list: FileMetadataList):
        is_integration_test = flt.Prefix("externalId", _FILE_PREFIX)

        count = cognite_client.documents.aggregate_cardinality_properties(
            path=SourceFileProperty.metadata, filter=is_integration_test
        )
        assert count >= len({k for doc in document_list for k in doc.metadata or []})

    @pytest.mark.usefixtures("document_list")
    def test_aggregate_unique_types(self, cognite_client: CogniteClient):
        is_integration_test = flt.Prefix("externalId", _FILE_PREFIX)

        is_not_text = agg.Not(agg.Prefix("text"))

        all_buckets = cognite_client.documents.aggregate_unique_values(
            property=DocumentProperty.mime_type, filter=is_integration_test
        )
        not_text_buckets = cognite_client.documents.aggregate_unique_values(
            property=DocumentProperty.mime_type,
            aggregate_filter=is_not_text,
            filter=is_integration_test,
        )
        assert not_text_buckets
        assert len(all_buckets) > len(not_text_buckets)

    @pytest.mark.usefixtures("document_list")
    def test_aggregate_unique_types__property_is_custom(self, cognite_client: CogniteClient):
        # Bug prior to 7.70.3 would convert user given property (list) to camelCase, which would then silently fail
        # and not return any results from the aggregation request.
        key_count = cognite_client.documents.aggregate_unique_values(
            property=SourceFileProperty.metadata_key("im_a_key")
        )
        # Note that the agg. API return keys in lowercase:
        assert {"p-duh-f", "t-x-t"} == set(res.values[0] for res in key_count)

        assert SourceFileProperty.mime_type.as_reference() == ["sourceFile", "mimeType"]
        mime_types = cognite_client.documents.aggregate_unique_values(property=SourceFileProperty.mime_type)
        assert {"text/plain", "application/pdf"} <= set(res.values[0] for res in mime_types)

        # When given in snake_case (wrong), we should ensure the agg. request fails (it means we havent converted it to camelCase)
        with pytest.raises(CogniteAPIError, match="Invalid property for the specified aggregate:") as err:
            cognite_client.documents.aggregate_unique_values(property=["source_file", "mime_type"])
        assert err.value.code == 400

    def test_aggregate_unique_metadata(self, cognite_client: CogniteClient, document_list: FileMetadataList):
        is_integration_test = flt.Prefix("externalId", _FILE_PREFIX)

        result = cognite_client.documents.aggregate_unique_properties(
            path=SourceFileProperty.metadata, filter=is_integration_test
        )
        assert result
        assert set(result.unique) >= {key.casefold() for a in document_list for key in a.metadata or []}

    def test_iterate_over_text_documents(self, cognite_client: CogniteClient):
        is_text_doc = flt.Equals(DocumentProperty.mime_type, "text/plain")

        for text_doc in cognite_client.documents(filter=is_text_doc):
            assert text_doc.mime_type == "text/plain"
            break

    def test_retrieve_pdf_link(self, cognite_client: CogniteClient, pdf_file: FileMetadata):
        link = cognite_client.documents.previews.retrieve_pdf_link(id=pdf_file.id)
        life = link.expires_at - int(time.time() * 1000)
        assert life > 0
        assert link.url.startswith("http")

    def test_download_image_bytes(self, cognite_client: CogniteClient, text_file: FileMetadata):
        content = cognite_client.documents.previews.download_page_as_png_bytes(id=text_file.id)
        assert content.startswith(b"\x89PNG\r\n\x1a\n")

    def test_download_pdf_bytes(self, cognite_client: CogniteClient, text_file: FileMetadata):
        content = cognite_client.documents.previews.download_document_as_pdf_bytes(id=text_file.id)
        assert content.startswith(b"%PDF")
