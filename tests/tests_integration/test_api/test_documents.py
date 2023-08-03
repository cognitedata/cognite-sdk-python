from __future__ import annotations

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.files import FileMetadata


@pytest.fixture(scope="session")
def file_content_pair(cognite_client: CogniteClient) -> tuple[FileMetadata, str]:
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


class TestDocumentsAPI:
    def test_list(self, cognite_client: CogniteClient):
        # Act
        documents = cognite_client.documents.list(limit=5)

        # Assert
        assert len(documents) > 0, "Expected to retrieve at least one document."

    def test_retrieve_content(self, cognite_client: CogniteClient, file_content_pair: tuple[FileMetadata, str]):
        # Arrange
        doc, content = file_content_pair

        # Act
        res = cognite_client.documents.retrieve_content(id=doc.id)

        # Assert
        assert res == content

    def test_search_no_filters_no_highlight(
        self, cognite_client: CogniteClient, file_content_pair: tuple[FileMetadata, str]
    ):
        # Arrange
        doc, content = file_content_pair
        query = '"pro at pericula ullamcorper"'

        # Act
        result = cognite_client.documents.search(query=query, limit=5)

        # Assert
        assert len(result) == 1, "Expected to retrieve exactly one document."
        actual = result[0]
        assert actual.id == doc.id
        assert actual.source_file.name == doc.name

    def test_search_no_filter_with_highlight(
        self, cognite_client: CogniteClient, file_content_pair: tuple[FileMetadata, str]
    ):
        # Arrange
        doc, content = file_content_pair
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
