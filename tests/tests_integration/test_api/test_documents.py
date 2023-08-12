from __future__ import annotations

import time
from pathlib import Path

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import aggregations, filters
from cognite.client.data_classes.documents import DocumentProperty, SortableDocumentProperty, SourceFileProperty
from cognite.client.data_classes.files import FileMetadata

RESOURCES = Path(__file__).resolve().parent / "documents_resources"


@pytest.fixture(scope="session")
def text_file_content_pair(cognite_client: CogniteClient) -> tuple[FileMetadata, str]:
    external_id = "document_api_integration_test_file"
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
    )
    return created_file, content


@pytest.fixture(scope="session")
def pdf_file(cognite_client: CogniteClient) -> FileMetadata:
    external_id = "document_api_integration_test_pdf_file"
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


class TestDocumentsAPI:
    def test_list(self, cognite_client: CogniteClient):
        # Act
        documents = cognite_client.documents.list(limit=5)

        # Assert
        assert len(documents) > 0, "Expected to retrieve at least one document."

    def test_list_lorem_ipsum(self, cognite_client: CogniteClient):
        # Arrange
        is_lorem = filters.Search(DocumentProperty.content, "lorem ipsum")

        # Act
        documents = cognite_client.documents.list(filter=is_lorem, limit=5)

        # Assert
        assert len(documents) > 0, "Expected to retrieve at least one document."

    def test_retrieve_content(self, cognite_client: CogniteClient, text_file_content_pair):
        # Arrange
        doc, content = text_file_content_pair

        # Act
        res = cognite_client.documents.retrieve_content(id=doc.id)

        # Assert
        assert res == content

    def test_search_no_filters_no_highlight(self, cognite_client: CogniteClient, text_file_content_pair):
        # Arrange
        doc, content = text_file_content_pair
        query = '"pro at pericula ullamcorper"'

        # Act
        result = cognite_client.documents.search(query=query, limit=5, sort=SortableDocumentProperty.title)

        # Assert
        assert len(result) == 1, "Expected to retrieve exactly one document."
        actual = result[0]
        assert actual.id == doc.id
        assert actual.source_file.name == doc.name

    def test_search_no_filter_with_highlight(self, cognite_client: CogniteClient, text_file_content_pair):
        # Arrange
        doc, content = text_file_content_pair
        query = '"pro at pericula ullamcorper"'

        # Act
        result = cognite_client.documents.search(query=query, highlight=True, limit=5)

        # Assert
        assert len(result) == 1, "Expected to retrieve exactly one document."
        actual = result[0]
        assert actual.document.id == doc.id
        assert actual.document.source_file.name == doc.name
        assert not actual.highlight.name
        assert query[1:-1] in actual.highlight.content[0]

    def test_aggregate_count(self, cognite_client: CogniteClient):
        # Act
        count = cognite_client.documents.aggregate_count()

        # Assert
        assert count > 0, "There should be at least one document in the test environment."

    def test_aggregate_cardinality(self, cognite_client: CogniteClient):
        # Act
        count = cognite_client.documents.aggregate_cardinality(property=DocumentProperty.type)

        # Assert
        assert count > 0

    def test_aggregate_cardinality_metadata(self, cognite_client: CogniteClient):
        # Act
        count = cognite_client.documents.aggregate_cardinality(property=SourceFileProperty.metadata)

        # Assert
        assert count > 0

    def test_aggregate_unique_types(self, cognite_client: CogniteClient):
        # Arrange
        a = aggregations
        is_not_text = a.Not(a.Prefix("text"))

        # Act
        all_buckets = cognite_client.documents.aggregate_unique(property=DocumentProperty.mime_type)
        not_text_buckets = cognite_client.documents.aggregate_unique(
            property=DocumentProperty.mime_type,
            aggregate_filter=is_not_text,
        )

        # Assert
        assert len(all_buckets) > len(not_text_buckets)

    def test_aggregate_unique_metadata(self, cognite_client: CogniteClient):
        # Act
        result = cognite_client.documents.aggregate_unique(property=SourceFileProperty.metadata)

        # Assert
        assert len(result) > 0

    def test_iterate_over_text_documents(self, cognite_client: CogniteClient):
        # Arrange
        is_text_doc = filters.Equals(DocumentProperty.mime_type, "text/plain")

        # Act
        for text_doc in cognite_client.documents(filter=is_text_doc):
            # Assert
            assert text_doc.mime_type == "text/plain"
            break

    def test_retrieve_pdf_link(self, cognite_client: CogniteClient, pdf_file: FileMetadata):
        # Act
        link = cognite_client.documents.preview.retrieve_pdf_link(id=pdf_file.id)

        # Assert
        life = link.expires_at - int(time.time() * 1000)
        assert life > 0
        assert link.url.startswith("http")

    def test_download_image_bytes(
        self, cognite_client: CogniteClient, text_file_content_pair: tuple[FileMetadata, str]
    ):
        # Arrange
        doc, _ = text_file_content_pair

        # Act
        content = cognite_client.documents.preview.download_page_as_png_bytes(id=doc.id)

        # Assert
        assert content.startswith(b"\x89PNG\r\n\x1a\n")

    def test_download_pdf_bytes(self, cognite_client: CogniteClient, text_file_content_pair: tuple[FileMetadata, str]):
        # Arrange
        doc, _ = text_file_content_pair

        # Act
        content = cognite_client.documents.preview.download_document_as_pdf_bytes(id=doc.id)

        # Assert
        assert content.startswith(b"%PDF")
