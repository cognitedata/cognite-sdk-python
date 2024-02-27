from __future__ import annotations

import re

import pytest

from cognite.client.data_classes import Document


@pytest.fixture
def mock_documents_response(rsps, cognite_client):
    response_body = {
        "items": [
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
            }
        ]
    }

    url_pattern = re.compile(re.escape(cognite_client.documents._get_base_url_with_base_path()) + "/.+")
    rsps.assert_all_requests_are_fired = False

    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
    yield rsps


class TestDocumentsAPI:
    def test_list(self, cognite_client, mock_documents_response):
        documents = cognite_client.documents.list()
        assert len(documents) == 1
        document = documents[0]
        assert isinstance(document, Document)
        assert mock_documents_response.calls[0].response.json()["items"][0] == document.dump(camel_case=True)
