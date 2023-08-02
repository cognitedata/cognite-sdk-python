from cognite.client import CogniteClient


class TestDocumentsAPI:
    def test_list(self, cognite_client: CogniteClient):
        # Act
        documents = cognite_client.documents.list(limit=5)

        # Assert
        assert len(documents) > 0, "Expected to retrieve at least one document."

    # def test_retrieve_content(self, cognite_client: CogniteClient):
    #     res = cognite_client.documents.retrieve_content(id=new_document.id)
    #     assert res == new_document.content
