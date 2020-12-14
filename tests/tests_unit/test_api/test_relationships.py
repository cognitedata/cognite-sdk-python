import gzip
import json
import re

import pytest

from cognite.client.beta import CogniteClient
from cognite.client.data_classes import Label, LabelFilter, Relationship, RelationshipList
from tests.utils import jsgz_load

COGNITE_CLIENT = CogniteClient()
REL_API = COGNITE_CLIENT.relationships


@pytest.fixture
def mock_rel_response(rsps):
    response_body = {
        "items": [
            {
                "externalId": "string",
                "sourceExternalId": "string",
                "sourceType": "asset",
                "targetExternalId": "string",
                "targetType": "asset",
                "startTime": 0,
                "endTime": 0,
                "confidence": 0,
                "dataSetId": 1,
                "createdTime": 0,
                "lastUpdatedTime": 0,
                "labels": {"externalId": "string"},
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
        assert {"items": [{"externalId": "a"}], "fetchResources": False} == jsgz_load(
            mock_rel_response.calls[0].request.body
        )

    def test_retrieve_multiple(self, mock_rel_response):
        res = REL_API.retrieve_multiple(external_ids=["a"])
        assert isinstance(res, RelationshipList)
        assert mock_rel_response.calls[0].response.json()["items"] == res.dump(camel_case=True)
        assert {"items": [{"externalId": "a"}], "fetchResources": False} == jsgz_load(
            mock_rel_response.calls[0].request.body
        )

    def test_list(self, mock_rel_response):
        res = REL_API.list()
        assert mock_rel_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_create_single(self, mock_rel_response):
        res = REL_API.create(
            Relationship(
                external_id="1",
                confidence=0.5,
                labels=[Label("belongsTo")],
                source_type="asset",
                source_external_id="source_ext_id",
                target_type="asset",
                target_external_id="bbb",
                data_set_id=12345,
            )
        )
        assert isinstance(res, Relationship)
        assert mock_rel_response.calls[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_create_single_types(self, mock_rel_response):
        types = ["asset", "timeSeries", "file", "event", "sequence"]
        for cls in types:
            res = REL_API.create(
                Relationship(
                    external_id="1",
                    confidence=0.5,
                    labels=[Label("belongsTo")],
                    source_type=cls,
                    source_external_id="source_ext_id",
                    target_type="asset",
                    target_external_id="bbb",
                )
            )
            assert isinstance(res, Relationship)
            res = REL_API.create(
                Relationship(
                    external_id="1",
                    confidence=0.5,
                    labels=[Label("belongsTo")],
                    source_type="asset",
                    source_external_id="foo",
                    target_type=cls,
                    target_external_id="bar",
                )
            )
            assert isinstance(res, Relationship)
            res = REL_API.create(
                Relationship(
                    external_id="1",
                    confidence=0.5,
                    labels=[Label("belongsTo")],
                    source_type=cls,
                    source_external_id="foo",
                    target_type=cls,
                    target_external_id="bar",
                )
            )
            assert isinstance(res, Relationship)

        for call in mock_rel_response.calls:
            x = json.loads(gzip.decompress(call.request.body).decode("utf-8"))["items"]
            it = x[0]
            assert isinstance(it["sourceType"], str)
            assert isinstance(it["targetType"], str)

    def test_create_wrong_type(self, mock_rel_response):
        with pytest.raises(TypeError):
            REL_API.create(
                Relationship(
                    external_id="1",
                    confidence=0.5,
                    labels=[Label("belongsTo")],
                    source_type="relationship",
                    source_external_id="foo",
                    target_type="asset",
                    target_external_id="bar",
                )
            )

    def test_create_multiple(self, mock_rel_response):
        rel1 = Relationship(
            external_id="new1",
            confidence=0.5,
            labels=[Label("flowsTo")],
            source_type="asset",
            source_external_id="foo",
            target_type="asset",
            target_external_id="bar",
        )
        rel2 = Relationship(
            external_id="new2",
            confidence=0.1,
            labels=[Label("flowsTo")],
            source_type="asset",
            source_external_id="foo",
            target_type="asset",
            target_external_id="bar",
        )
        res = REL_API.create([rel1, rel2])
        assert isinstance(res, RelationshipList)
        assert mock_rel_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_iter_single(self, mock_rel_response):
        for rel in REL_API:
            assert mock_rel_response.calls[0].response.json()["items"][0] == rel.dump(camel_case=True)

    def test_delete_single(self, mock_rel_response):
        res = REL_API.delete(external_id="a")
        assert {"items": [{"externalId": "a"}]} == jsgz_load(mock_rel_response.calls[0].request.body)
        assert res is None

    def test_delete_multiple(self, mock_rel_response):
        res = REL_API.delete(external_id=["a"])
        assert {"items": [{"externalId": "a"}]} == jsgz_load(mock_rel_response.calls[0].request.body)
        assert res is None

    def test_advanced_list(self, mock_rel_response):
        res = REL_API.list(source_types=["asset"], labels=LabelFilter(contains_any=["label_ext_id"]))
        assert {
            "filter": {"sourceTypes": ["asset"], "labels": {"containsAny": [{"externalId": "label_ext_id"}]}},
            "limit": 100,
            "cursor": None,
            "fetchResources": False,
        } == jsgz_load(mock_rel_response.calls[0].request.body)
        assert mock_rel_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_source_target_packing(self, mock_rel_response):
        res = REL_API.list(
            source_types=["asset"],
            source_external_ids=["bla"],
            target_types=["timeseries"],
            target_external_ids=["foo"],
            labels=LabelFilter(contains_any=["belongs_to"]),
        )
        assert {
            "filter": {
                "sourceTypes": ["asset"],
                "sourceExternalIds": ["bla"],
                "targetTypes": ["timeseries"],
                "targetExternalIds": ["foo"],
                "labels": {"containsAny": [{"externalId": "belongs_to"}]},
            },
            "limit": 100,
            "cursor": None,
            "fetchResources": False,
        } == jsgz_load(mock_rel_response.calls[0].request.body)
        assert mock_rel_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_multi_source_target_list(self, mock_rel_response):
        source_external_ids = ["source1", "source2"]
        source_types = ["asset", "asset"]
        target_external_ids = ["target1", "target2"]
        target_types = ["event", "event"]
        data_sets = [{"id": 1234}, {"externalId": "test_dataSet_id"}]
        data_set_ids = [1234]
        data_set_external_ids = ["test_dataSet_id"]
        created_time = 1565965333132
        last_updated_time = 1565965333132
        res = REL_API.list(
            source_external_ids=source_external_ids,
            source_types=source_types,
            target_external_ids=target_external_ids,
            target_types=target_types,
            data_set_ids=data_set_ids,
            data_set_external_ids=data_set_external_ids,
            created_time=created_time,
            last_updated_time=last_updated_time,
        )
        assert {
            "filter": {
                "createdTime": created_time,
                "lastUpdatedTime": last_updated_time,
                "sourceTypes": source_types,
                "sourceExternalIds": source_external_ids,
                "targetTypes": target_types,
                "targetExternalIds": target_external_ids,
                "dataSetIds": data_sets,
            },
            "limit": 100,
            "cursor": None,
            "fetchResources": False,
        } == jsgz_load(mock_rel_response.calls[0].request.body)
        assert mock_rel_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_many_source_targets(self, mock_rel_response):
        source_external_ids = [str(i) for i in range(2500)]
        target_external_ids = [str(i) for i in range(3500)]
        with pytest.raises(ValueError):
            REL_API(source_external_ids=source_external_ids, target_external_ids=target_external_ids)
        with pytest.raises(ValueError):
            REL_API.list(source_external_ids=source_external_ids, target_external_ids=target_external_ids)
        res = REL_API.list(source_external_ids=source_external_ids, target_external_ids=target_external_ids, limit=None)
        assert 12 == len(mock_rel_response.calls)
        assert isinstance(res, RelationshipList)
        assert 12 == len(res)

    def test_many_sources_only(self, mock_rel_response):
        source_external_ids = [str(i) for i in range(2500)]
        with pytest.raises(ValueError):
            REL_API(source_external_ids=source_external_ids)

        res = REL_API.list(source_external_ids=source_external_ids, limit=-1)
        assert 3 == len(mock_rel_response.calls)
        assert isinstance(res, RelationshipList)
        assert 3 == len(res)
        requested_sources = []
        for call in mock_rel_response.calls:
            json = jsgz_load(call.request.body)
            assert "targetExternalIds" not in json["filter"]
            requested_sources.extend([s for s in json["filter"]["sourceExternalIds"]])
        assert set([s for s in source_external_ids]) == set(requested_sources)
