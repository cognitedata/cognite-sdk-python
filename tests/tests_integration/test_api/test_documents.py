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

    def test_retrieve_content(self, cognite_client: CogniteClient, file_content_pair):
        # Arrange
        doc, content = file_content_pair

        res = cognite_client.documents.retrieve_content(id=doc.id)
        assert res == content
