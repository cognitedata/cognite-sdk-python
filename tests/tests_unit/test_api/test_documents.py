from __future__ import annotations

import re

import pytest

from cognite.client.data_classes import Document
from tests.utils import get_url


@pytest.fixture
def example_document():
    return {
        "id": 952558513813,
        "sourceFile": {
            "name": "04.pdf",
            "source": "foo",
            "hash": "4ad0156942d1c26a4aa74fe6d52c082c6871b33b0e933f40566f6740fdae0ebb",
            "assetIds": [2728768807111995],
            "metadata": {"myKey": "myValue"},
            "geoLocation": {
                "type": "Polygon",
                "coordinates": [[[3.0, 1.0], [4.0, 4.0], [2.0, 4.0], [1.0, 2.0], [3.0, 1.0]]],
            },
        },
        "externalId": "7a05f794-d6b0-413a-a0ff-c03eb38d9e83",
        "title": "Sample Scanned Image",
        "author": "Paperless 800-387-9001",
        "producer": "Imaging Dept.",
        "mimeType": "application/pdf",
        "extension": "pdf",
        "pageCount": 1,
        "type": "PDF",
        "language": "en",
        "truncatedContent": "test",
        "assetIds": [2728768807111995, 7234953846172358],
        "labels": [],
        "createdTime": 1659617852965,
        "modifiedTime": 970589816000,
        "lastIndexedTime": 1707210718089,
        "geoLocation": {
            "type": "GeometryCollection",
            "geometries": [
                {"type": "Point", "coordinates": [-2.3, 50.8]},
                {
                    "type": "Polygon",
                    "coordinates": [[[3.0, 1.0], [4.0, 4.0], [2.0, 4.0], [1.0, 2.0], [3.0, 1.0]]],
                },
            ],
        },
    }

@pytest.fixture
def mock_documents_response(httpx_mock, cognite_client, example_document):
    response_body = {"items":[example_document]}
    url_pattern = re.compile(re.escape(get_url(cognite_client.documents)) + "/.+")

    httpx_mock.add_response(method="POST", url=url_pattern, status_code=200, json=response_body, is_optional=True)
    httpx_mock.add_response(method="GET", url=url_pattern, status_code=200, json=response_body, is_optional=True)

    yield httpx_mock


class TestDocumentsAPI:
    def test_list(self, cognite_client, mock_documents_response, example_document):
        documents = cognite_client.documents.list()
        assert len(documents) == 1
        document = documents[0]
        assert isinstance(document, Document)
        assert example_document == document.dump(camel_case=True)
