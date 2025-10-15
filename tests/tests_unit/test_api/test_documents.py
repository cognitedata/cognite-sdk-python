from __future__ import annotations

import re
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

import pytest
from pytest_httpx import HTTPXMock

from cognite.client import CogniteClient
from cognite.client.data_classes import Document
from tests.utils import get_url

if TYPE_CHECKING:
    from pytest_httpx import HTTPXMock

    from cognite.client import AsyncCogniteClient, CogniteClient


@pytest.fixture
def example_documents() -> list[dict[str, Any]]:
    return [
        {
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
            "instanceId": {
                "space": "demo-space",
                "externalId": "7a05f794-d6b0-413a-a0ff-c03eb38d9e83",
            },
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
        },
        {
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
        },
    ]


@pytest.fixture
def mock_documents_list_response(
    httpx_mock: HTTPXMock,
    cognite_client: CogniteClient,
    example_documents: list[dict[str, Any]],
    async_client: AsyncCogniteClient,
) -> HTTPXMock:
    response_body = {"items": example_documents}
    url_pattern = re.compile(re.escape(get_url(async_client.documents)) + "/.+")

    httpx_mock.add_response(method="POST", url=url_pattern, status_code=200, json=response_body, is_optional=True)
    httpx_mock.add_response(method="GET", url=url_pattern, status_code=200, json=response_body, is_optional=True)

    return httpx_mock


@pytest.fixture
def mock_documents_search_response(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> Iterator[HTTPXMock]:
    response_body = {
        "items": [
            {
                "item": {
                    "id": 952558513813,
                    "sourceFile": {"name": "04.pdf"},
                    "externalId": "7a05f794-d6b0-413a-a0ff-c03eb38d9e83",
                    "type": "PDF",
                    "truncatedContent": "test",
                    "createdTime": 1659617852965,
                    "modifiedTime": 970589816000,
                    "lastIndexedTime": 1707210718089,
                },
            },
        ]
    }
    response_body_with_cursor = response_body | {"nextCursor": "foo"}

    url_pattern = re.compile(re.escape(get_url(async_client.documents)) + "/.+")

    # Add responses for the search endpoint
    httpx_mock.add_response(
        method="POST", url=url_pattern, status_code=200, json=response_body_with_cursor, is_optional=True
    )
    httpx_mock.add_response(
        method="POST", url=url_pattern, status_code=200, json=response_body_with_cursor, is_optional=True
    )
    httpx_mock.add_response(method="POST", url=url_pattern, status_code=200, json=response_body, is_optional=True)

    yield httpx_mock


class TestDocumentsAPI:
    @pytest.mark.usefixtures("mock_documents_list_response")
    def test_list(self, cognite_client: CogniteClient, example_documents: list[dict[str, Any]]) -> None:
        documents = cognite_client.documents.list()
        assert len(documents) == 2
        file_with_instance_id, file_wo_instance_id = documents
        assert isinstance(file_with_instance_id, Document)
        assert isinstance(file_wo_instance_id, Document)
        assert file_with_instance_id.instance_id is not None
        assert file_with_instance_id.instance_id.space == "demo-space"
        assert file_with_instance_id.instance_id.external_id == "7a05f794-d6b0-413a-a0ff-c03eb38d9e83"
        assert file_wo_instance_id.instance_id is None

        doc1, doc2 = example_documents
        assert doc1 == file_with_instance_id.dump(camel_case=True)
        assert doc2 == file_wo_instance_id.dump(camel_case=True)

    @pytest.mark.usefixtures("mock_documents_search_response")
    def test_search_limit(self, cognite_client: CogniteClient) -> None:
        documents = cognite_client.documents.search(query="a", limit=1)
        assert len(documents) == 1
