import gzip
import json
import re

import pytest
from httpx import Request as HttpxRequest 
from httpx import Response as HttpxResponse 


from cognite.client.data_classes import Label, LabelFilter, Relationship, RelationshipList, RelationshipUpdate
from tests.utils import jsgz_load


@pytest.fixture
def mock_rel_response(respx_mock, cognite_client):
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
                "labels": [{"externalId": "string"}],
            }
        ]
    }
    url_pattern = re.compile(
        re.escape(cognite_client.relationships._get_base_url_with_base_path())
        + r"/relationships(?:/byids|/update|/delete|/list|/search|$|\?.+)"
    )
    
    respx_mock.post(url__regex=url_pattern).respond(status_code=200, json=response_body)
    respx_mock.get(url__regex=url_pattern).respond(status_code=200, json=response_body)
    yield respx_mock


@pytest.fixture
def mock_rel_empty(respx_mock, cognite_client):
    response_body = {"items": []}
    url_pattern = re.compile(
        re.escape(cognite_client.relationships._get_base_url_with_base_path())
        + r"/relationships(?:/byids|/update|/delete|/list|/search|$|\?.+)"
    )
    
    respx_mock.post(url__regex=url_pattern).respond(status_code=200, json=response_body)
    respx_mock.get(url__regex=url_pattern).respond(status_code=200, json=response_body)
    yield respx_mock


class TestRelationships:
    def test_retrieve_single(self, cognite_client, mock_rel_response):
        res = cognite_client.relationships.retrieve(external_id="a")
        assert isinstance(res, Relationship)
        assert mock_rel_response.calls.last.response.json()["items"][0] == res.dump(camel_case=True)
        assert {"items": [{"externalId": "a"}], "fetchResources": False} == jsgz_load(
            mock_rel_response.calls.last.request.content
        )

    def test_retrieve_multiple(self, cognite_client, mock_rel_response):
        res = cognite_client.relationships.retrieve_multiple(external_ids=["a"])
        assert isinstance(res, RelationshipList)
        assert mock_rel_response.calls.last.response.json()["items"] == res.dump(camel_case=True)
        assert {"items": [{"externalId": "a"}], "fetchResources": False, "ignoreUnknownIds": False} == jsgz_load(
            mock_rel_response.calls.last.request.content
        )

    def test_list(self, cognite_client, mock_rel_response):
        res = cognite_client.relationships.list()
        assert mock_rel_response.calls.last.response.json()["items"] == res.dump(camel_case=True)

    def test_create_single(self, cognite_client, mock_rel_response):
        res = cognite_client.relationships.create(
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
        assert mock_rel_response.calls.last.response.json()["items"][0] == res.dump(camel_case=True)

    def test_create_single_types(self, cognite_client, mock_rel_response):
        types = ["asset", "timeSeries", "file", "event", "sequence"]
        for cls in types:
            res = cognite_client.relationships.create(
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
            res = cognite_client.relationships.create(
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
            res = cognite_client.relationships.create(
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
            if call.request.method == "POST": # Only check POST requests
                x = json.loads(gzip.decompress(call.request.content).decode("utf-8"))["items"]
                it = x[0]
                assert isinstance(it["sourceType"], str)
                assert isinstance(it["targetType"], str)

    def test_create_wrong_type(self, cognite_client, mock_rel_response):
        with pytest.raises(TypeError):
            cognite_client.relationships.create(
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

    def test_create_multiple(self, cognite_client, mock_rel_response):
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
        res = cognite_client.relationships.create([rel1, rel2])
        assert isinstance(res, RelationshipList)
        assert mock_rel_response.calls.last.response.json()["items"] == res.dump(camel_case=True)

    def test_update_with_resource_class(self, cognite_client, mock_rel_response):
        res = cognite_client.relationships.update(Relationship(external_id="test_1"))
        assert isinstance(res, Relationship)
        assert mock_rel_response.calls.last.response.json()["items"][0] == res.dump(camel_case=True)

    def test_update_with_update_class(self, cognite_client, mock_rel_response):
        res = cognite_client.relationships.update(RelationshipUpdate(external_id="test_1").confidence.set(None))
        assert isinstance(res, Relationship)
        assert mock_rel_response.calls.last.response.json()["items"][0] == res.dump(camel_case=True)

    def test_update_multiple(self, cognite_client, mock_rel_response):
        res = cognite_client.relationships.update(
            [
                RelationshipUpdate(external_id="test1").source_external_id.set("blabla"),
                RelationshipUpdate(external_id="test2").source_external_id.set("blabla"),
            ]
        )
        assert isinstance(res, RelationshipList)
        assert mock_rel_response.calls.last.response.json()["items"] == res.dump(camel_case=True)

    def test_update_labels_single(self, cognite_client, mock_rel_response):
        cognite_client.relationships.update(
            [RelationshipUpdate(external_id="test1").labels.add("PUMP").labels.remove("VALVE")]
        )
        expected = {"labels": {"add": [{"externalId": "PUMP"}], "remove": [{"externalId": "VALVE"}]}}
        assert expected == jsgz_load(mock_rel_response.calls.last.request.content)["items"][0]["update"]

    def test_update_labels_multiple(self, cognite_client, mock_rel_response):
        cognite_client.relationships.update(
            [
                RelationshipUpdate(external_id="test1")
                .labels.add(["PUMP", "ROTATING_EQUIPMENT"])
                .labels.remove(["VALVE", "VERIFIED"])
            ]
        )
        expected = {
            "labels": {
                "add": [{"externalId": "PUMP"}, {"externalId": "ROTATING_EQUIPMENT"}],
                "remove": [{"externalId": "VALVE"}, {"externalId": "VERIFIED"}],
            }
        }
        assert expected == jsgz_load(mock_rel_response.calls.last.request.content)["items"][0]["update"]

    def test_update_labels_resource_class(self, cognite_client, mock_rel_response):
        cognite_client.relationships.update(
            Relationship(external_id="test1", labels=[Label(external_id="Pump")], source_external_id="source1")
        )
        expected = {"sourceExternalId": {"set": "source1"}, "labels": {"set": [{"externalId": "Pump"}]}}
        assert expected == jsgz_load(mock_rel_response.calls.last.request.content)["items"][0]["update"]

    def test_iter_single(self, cognite_client, mock_rel_response):
        for rel in cognite_client.relationships:
            assert mock_rel_response.calls.last.response.json()["items"][0] == rel.dump(camel_case=True)

    def test_delete_single(self, cognite_client, mock_rel_response):
        res = cognite_client.relationships.delete(external_id="a")
        assert {"items": [{"externalId": "a"}], "ignoreUnknownIds": False} == jsgz_load(
            mock_rel_response.calls.last.request.content
        )
        assert res is None

    def test_delete_multiple(self, cognite_client, mock_rel_response):
        res = cognite_client.relationships.delete(external_id=["a"])
        assert {"items": [{"externalId": "a"}], "ignoreUnknownIds": False} == jsgz_load(
            mock_rel_response.calls.last.request.content
        )
        assert res is None

    def test_delete_multiple_ignore_unknown_ids(self, cognite_client, mock_rel_response):
        res = cognite_client.relationships.delete(external_id=[1], ignore_unknown_ids=True) # type: ignore[list-item]
        assert {"items": [{"externalId": "1"}], "ignoreUnknownIds": True} == jsgz_load(
            mock_rel_response.calls.last.request.content
        )
        assert res is None

    def test_advanced_list(self, cognite_client, mock_rel_response):
        res = cognite_client.relationships.list(
            source_types=["asset"], labels=LabelFilter(contains_any=["label_ext_id"])
        )
        assert {
            "filter": {"sourceTypes": ["asset"], "labels": {"containsAny": [{"externalId": "label_ext_id"}]}},
            "limit": 25,
            "cursor": None,
            "fetchResources": False,
        } == jsgz_load(mock_rel_response.calls.last.request.content)
        assert mock_rel_response.calls.last.response.json()["items"] == res.dump(camel_case=True)

    def test_source_target_packing(self, cognite_client, mock_rel_response):
        res = cognite_client.relationships.list(
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
            "limit": 25,
            "cursor": None,
            "fetchResources": False,
        } == jsgz_load(mock_rel_response.calls.last.request.content)
        assert mock_rel_response.calls.last.response.json()["items"] == res.dump(camel_case=True)

    def test_multi_source_target_list(self, cognite_client, mock_rel_response):
        source_external_ids = ["source1", "source2"]
        source_types = ["asset", "asset"]
        target_external_ids = ["target1", "target2"]
        target_types = ["event", "event"]
        data_sets = [{"id": 1234}, {"externalId": "test_dataSet_id"}]
        data_set_ids = [1234]
        data_set_external_ids = ["test_dataSet_id"]
        created_time = 1565965333132
        last_updated_time = 1565965333132
        res = cognite_client.relationships.list(
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
            "limit": 25,
            "cursor": None,
            "fetchResources": False,
        } == jsgz_load(mock_rel_response.calls.last.request.content)
        assert mock_rel_response.calls.last.response.json()["items"] == res.dump(camel_case=True)

    def test_many_source_targets(self, cognite_client, mock_rel_response):
        source_external_ids = [str(i) for i in range(2500)]
        target_external_ids = [str(i) for i in range(3500)]
        with pytest.raises(ValueError):
            cognite_client.relationships(
                source_external_ids=source_external_ids, target_external_ids=target_external_ids
            )
        with pytest.raises(ValueError):
            cognite_client.relationships.list(
                source_external_ids=source_external_ids, target_external_ids=target_external_ids
            )
        res = cognite_client.relationships.list(
            source_external_ids=source_external_ids, target_external_ids=target_external_ids, limit=None
        )
        assert 12 == len(mock_rel_response.calls)
        assert isinstance(res, RelationshipList)
        assert 12 == len(res)

    def test_many_sources_only(self, cognite_client, mock_rel_response):
        source_external_ids = [str(i) for i in range(2500)]
        with pytest.raises(ValueError):
            cognite_client.relationships(source_external_ids=source_external_ids)

        res = cognite_client.relationships.list(source_external_ids=source_external_ids, limit=-1)
        assert 3 == len(mock_rel_response.calls)
        assert isinstance(res, RelationshipList)
        assert 3 == len(res)
        requested_sources = []
        for call in mock_rel_response.calls:
            json_body = jsgz_load(call.request.content)
            assert "targetExternalIds" not in json_body["filter"]
            requested_sources.extend([s for s in json_body["filter"]["sourceExternalIds"]])
        assert {s for s in source_external_ids} == set(requested_sources)

[end of tests/tests_unit/test_api/test_relationships.py]
