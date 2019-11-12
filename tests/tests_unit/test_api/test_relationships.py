import gzip
import json
import re

import pytest

from cognite.client.data_classes import (
    Asset,
    Event,
    FileMetadata,
    Relationship,
    RelationshipFilter,
    RelationshipList,
    Sequence,
    TimeSeries,
)
from cognite.client.experimental import CogniteClient
from tests.utils import jsgz_load

COGNITE_CLIENT = CogniteClient()
REL_API = COGNITE_CLIENT.relationships


@pytest.fixture
def mock_rel_response(rsps):
    response_body = {
        "items": [
            {
                "externalId": "rel-123",
                "createdTime": 1565965333132,
                "lastUpdatedTime": 1565965333132,
                "confidence": 0.99,
                "dataSet": "testSource",
                "relationshipType": "flowsTo",
                "source": {"resourceId": "asset1", "resource": "Asset"},
                "target": {"resourceId": "asset2", "resource": "Asset"},
            }
        ]
    }
    url_pattern = re.compile(
        re.escape(REL_API._get_base_url_with_base_path())
        + r"/relationships(?:/byids|/update|/delete|/list|/search|$|\?.+)"
    )
    rsps.assert_all_requests_are_fired = False

    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
    yield rsps


@pytest.fixture
def mock_rel_empty(rsps):
    response_body = {"items": []}
    url_pattern = re.compile(
        re.escape(REL_API._get_base_url_with_base_path())
        + r"/relationships(?:/byids|/update|/delete|/list|/search|$|\?.+)"
    )
    rsps.assert_all_requests_are_fired = False

    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
    yield rsps


class TestRelationships:
    def test_retrieve_single(self, mock_rel_response):
        res = REL_API.retrieve(external_id="a")
        assert isinstance(res, Relationship)
        assert mock_rel_response.calls[0].response.json()["items"][0] == res.dump(camel_case=True)
        assert {"items": [{"externalId": "a"}]} == jsgz_load(mock_rel_response.calls[0].request.body)

    def test_retrieve_multiple(self, mock_rel_response):
        res = REL_API.retrieve_multiple(external_ids=["a"])
        assert isinstance(res, RelationshipList)
        assert mock_rel_response.calls[0].response.json()["items"] == res.dump(camel_case=True)
        assert {"items": [{"externalId": "a"}]} == jsgz_load(mock_rel_response.calls[0].request.body)

    def test_list(self, mock_rel_response):
        res = REL_API.list()
        assert mock_rel_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_create_single(self, mock_rel_response):
        res = REL_API.create(
            Relationship(
                external_id="1",
                confidence=0.5,
                relationship_type="flowsTo",
                source={"resourceId": "aaa", "resource": "Asset"},
                target={"resourceId": "bbb", "resource": "Asset"},
            )
        )
        assert isinstance(res, Relationship)
        assert mock_rel_response.calls[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_create_single_types(self, mock_rel_response):
        types = [Asset, TimeSeries, FileMetadata, Event, Sequence]
        for cls in types:
            test = cls(external_id="test")
            res = REL_API.create(
                Relationship(
                    external_id="1",
                    confidence=0.5,
                    relationship_type="belongsTo",
                    source=test,
                    target={"resourceId": "bbb", "resource": "Asset"},
                )
            )
            assert isinstance(res, Relationship)
            res = REL_API.create(
                Relationship(
                    external_id="1",
                    confidence=0.5,
                    relationship_type="belongsTo",
                    source={"resourceId": "bbb", "resource": "Asset"},
                    target=test,
                )
            )
            assert isinstance(res, Relationship)
            res = REL_API.create(
                Relationship(external_id="1", confidence=0.5, relationship_type="belongsTo", source=test, target=test)
            )
            assert isinstance(res, Relationship)

        for call in mock_rel_response.calls:
            it = json.loads(gzip.decompress(call.request.body).decode("utf-8"))["items"][0]
            assert isinstance(it["source"], dict)
            assert isinstance(it["target"], dict)

    def test_create_wrong_type(self, mock_rel_response):
        with pytest.raises(ValueError):
            REL_API.create(
                Relationship(
                    external_id="1",
                    confidence=0.5,
                    relationship_type="belongsTo",
                    source=Relationship(external_id="a"),
                    target={"resourceId": "bbb", "resource": "Asset"},
                )
            )

    def test_create_multiple(self, mock_rel_response):
        rel1 = Relationship(
            external_id="new1",
            confidence=0.5,
            relationship_type="flowsTo",
            source={"resourceId": "aaa", "resource": "Asset"},
            target={"resourceId": "bbb", "resource": "Asset"},
        )
        rel2 = Relationship(
            external_id="new2",
            confidence=0.1,
            relationship_type="flowsTo",
            source={"resourceId": "aaa", "resource": "Asset"},
            target={"resourceId": "bbb", "resource": "Asset"},
        )
        res = REL_API.create([rel1, rel2])
        assert isinstance(res, RelationshipList)
        assert mock_rel_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_iter_single(self, mock_rel_response):
        for rel in REL_API:
            assert mock_rel_response.calls[0].response.json()["items"][0] == rel.dump(camel_case=True)

    def test_iter_chunk(self, mock_rel_response):
        for rel in REL_API(chunk_size=1, data_set="ds"):
            assert mock_rel_response.calls[0].response.json()["items"] == rel.dump(camel_case=True)

    def test_delete_single(self, mock_rel_response):
        res = REL_API.delete(external_id="a")
        assert {"items": [{"externalId": "a"}]} == jsgz_load(mock_rel_response.calls[0].request.body)
        assert res is None

    def test_delete_multiple(self, mock_rel_response):
        res = REL_API.delete(external_id=["a"])
        assert {"items": [{"externalId": "a"}]} == jsgz_load(mock_rel_response.calls[0].request.body)
        assert res is None

    def test_advanced_list(self, mock_rel_response):
        res = REL_API.list(source_resource="asset", relationship_type="belongs_to")
        assert {"filter": {"sourceResource": "asset", "relationshipType": "belongs_to"}, "limit": 25} == jsgz_load(
            mock_rel_response.calls[0].request.body
        )
        assert mock_rel_response.calls[0].response.json()["items"] == res.dump(camel_case=True)
