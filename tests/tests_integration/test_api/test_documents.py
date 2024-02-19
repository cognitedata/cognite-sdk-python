from __future__ import annotations

import inspect
import time
from io import BytesIO
from pathlib import Path

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import aggregations, filters
from cognite.client.data_classes.documents import (
    DocumentProperty,
    SortableDocumentProperty,
    SourceFile,
    SourceFileProperty,
)
from cognite.client.data_classes.files import FileMetadata, FileMetadataList
from tests.utils import dict_without

RESOURCES = Path(__file__).resolve().parent / "documents_resources"

_FILE_PREFIX = "document_api_integration"

_SYMMETRIC_DIFFERENCE_FILEMETADATA_SOURCEFILE: frozenset[str] = frozenset(
    set(inspect.signature(FileMetadata.__init__).parameters) ^ set(inspect.signature(SourceFile.__init__).parameters)
)


@pytest.fixture(scope="session")
def text_file_content_pair(cognite_client: CogniteClient) -> tuple[FileMetadata, str]:
    external_id = f"{_FILE_PREFIX}_test_file"
    content = """This is a test file for the document API integration test.

Lorem ipsum dolor sit amet, eum an quando noster graecis, nam everti offendit an.
Quod probo mazim cu pro, pro at pericula ullamcorper, vitae cetero vis ex.
Error deserunt eu eos, te vix labores suscipiantur, ut quot atomorum comprehensam quo.
Nam cu appareat officiis, vis et enim vide possit. Dicat bonorum ancillae est at.
Mei et possim option. An sit ipsum scaevola."""
    file = cognite_client.files.retrieve(external_id=external_id)
    if file is not None:
        return file, content
    created_file = cognite_client.files.upload_bytes(
        content=content,
        name=external_id,
        external_id=external_id,
        mime_type="text/plain",
        metadata={"solver_type": "linear"},
    )
    return created_file, content


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
    )
    return created_file


@pytest.fixture(scope="session")
def document_list(text_file_content_pair: tuple[FileMetadata, str], pdf_file: FileMetadata) -> FileMetadataList:
    return FileMetadataList([text_file_content_pair[0], pdf_file])


class TestDocumentsAPI:
    def test_list(
        self,
        cognite_client: CogniteClient,
        document_list: FileMetadataList,
        text_file_content_pair: tuple[FileMetadata, str],
    ):
        text_file, _ = text_file_content_pair
        is_integration_test = filters.Prefix("externalId", _FILE_PREFIX)

        documents = cognite_client.documents.list(
            limit=5, filter=is_integration_test, sort=SortableDocumentProperty.mime_type
        )

        assert len(documents) >= len(document_list)
        assert sorted(doc.mime_type for doc in documents) == sorted(doc.mime_type for doc in document_list)
        exclude = set(_SYMMETRIC_DIFFERENCE_FILEMETADATA_SOURCEFILE)
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
        is_integration_test = filters.Prefix("externalId", _FILE_PREFIX)
        is_lorem = filters.Search(DocumentProperty.content, "lorem ipsum")

        documents = cognite_client.documents.list(filter=filters.And(is_lorem, is_integration_test), limit=5)

        # Both the files in the document list have "lorem ipsum" in the content
        assert len(documents) >= len(document_list)
        retrieved_pdf = documents.get(id=pdf_file.id)
        assert retrieved_pdf is not None, "Expected to retrieve the pdf file to be the list"
        exclude = set(_SYMMETRIC_DIFFERENCE_FILEMETADATA_SOURCEFILE)
        assert dict_without(retrieved_pdf.source_file.dump(camel_case=False), exclude) == dict_without(
            pdf_file.dump(camel_case=False), exclude
        )

    def test_retrieve_content(self, cognite_client: CogniteClient, text_file_content_pair):
        doc, content = text_file_content_pair

        res = cognite_client.documents.retrieve_content(id=doc.id)

        assert isinstance(res, bytes)
        res = res.decode("utf-8")
        assert res == content

    def test_retrieve_content_into_buffer(self, cognite_client: CogniteClient, text_file_content_pair):
        doc, content = text_file_content_pair
        buffer = BytesIO()

        res = cognite_client.documents.retrieve_content_buffer(id=doc.id, buffer=buffer)

        assert res is None
        assert buffer.getvalue().decode("utf-8") == content

    def test_search_no_filters_no_highlight(self, cognite_client: CogniteClient, text_file_content_pair):
        doc, content = text_file_content_pair
        query = '"pro at pericula ullamcorper"'

        result = cognite_client.documents.search(query=query, limit=5, sort=SortableDocumentProperty.title)

        assert len(result) == 1, "Expected to retrieve exactly one document."
        actual = result[0]
        assert actual.id == doc.id
        assert actual.source_file.name == doc.name

    def test_search_no_filter_with_highlight(self, cognite_client: CogniteClient, text_file_content_pair):
        doc, content = text_file_content_pair
        query = '"pro at pericula ullamcorper"'

        result = cognite_client.documents.search(query=query, highlight=True, limit=5)

        assert len(result) == 1, "Expected to retrieve exactly one document."
        actual = result[0]
        assert actual.document.id == doc.id
        assert actual.document.source_file.name == doc.name
        assert not actual.highlight.name
        assert query[1:-1] in actual.highlight.content[0]

    def test_aggregate_count(self, cognite_client: CogniteClient, document_list: FileMetadataList):
        f = filters
        is_integration_test = f.Prefix("externalId", _FILE_PREFIX)

        count = cognite_client.documents.aggregate_count(filter=is_integration_test)

        assert count >= len(document_list)

    def test_aggregate_cardinality(self, cognite_client: CogniteClient, document_list: FileMetadataList):
        f = filters
        is_integration_test = f.Prefix("externalId", _FILE_PREFIX)

        count = cognite_client.documents.aggregate_cardinality_values(
            property=DocumentProperty.mime_type, filter=is_integration_test
        )

        assert count >= len({doc.mime_type for doc in document_list if doc.mime_type is not None})

    def test_aggregate_cardinality_metadata(self, cognite_client: CogniteClient, document_list: FileMetadataList):
        f = filters
        is_integration_test = f.Prefix("externalId", _FILE_PREFIX)

        count = cognite_client.documents.aggregate_cardinality_properties(
            path=SourceFileProperty.metadata, filter=is_integration_test
        )

        assert count >= len({k for doc in document_list for k in doc.metadata or []})

    def test_aggregate_unique_types(self, cognite_client: CogniteClient, document_list: FileMetadataList):
        f = filters
        is_integration_test = f.Prefix("externalId", _FILE_PREFIX)

        agg = aggregations
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

    def test_aggregate_unique_metadata(self, cognite_client: CogniteClient, document_list: FileMetadataList):
        f = filters
        is_integration_test = f.Prefix("externalId", _FILE_PREFIX)

        result = cognite_client.documents.aggregate_unique_properties(
            path=SourceFileProperty.metadata, filter=is_integration_test
        )

        assert result
        assert set(result.unique) >= {key.casefold() for a in document_list for key in a.metadata or []}

    def test_iterate_over_text_documents(self, cognite_client: CogniteClient):
        is_text_doc = filters.Equals(DocumentProperty.mime_type, "text/plain")

        for text_doc in cognite_client.documents(filter=is_text_doc):
            assert text_doc.mime_type == "text/plain"
            break

    def test_retrieve_pdf_link(self, cognite_client: CogniteClient, pdf_file: FileMetadata):
        link = cognite_client.documents.previews.retrieve_pdf_link(id=pdf_file.id)

        life = link.expires_at - int(time.time() * 1000)
        assert life > 0
        assert link.url.startswith("http")

    def test_download_image_bytes(
        self, cognite_client: CogniteClient, text_file_content_pair: tuple[FileMetadata, str]
    ):
        doc, _ = text_file_content_pair

        content = cognite_client.documents.previews.download_page_as_png_bytes(id=doc.id)

        assert content.startswith(b"\x89PNG\r\n\x1a\n")

    def test_download_pdf_bytes(self, cognite_client: CogniteClient, text_file_content_pair: tuple[FileMetadata, str]):
        doc, _ = text_file_content_pair

        content = cognite_client.documents.previews.download_document_as_pdf_bytes(id=doc.id)

        assert content.startswith(b"%PDF")
