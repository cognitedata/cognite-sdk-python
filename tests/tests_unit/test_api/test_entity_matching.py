import re

import pytest

from cognite.client.data_classes import Asset, EntityMatchingModel, TimeSeries
from cognite.client.exceptions import ModelFailedException
from tests.utils import jsgz_load


@pytest.fixture
def mock_fit(rsps, cognite_client):
    response_body = {"id": 123, "status": "Queued", "createdTime": 42}
    rsps.add(
        rsps.POST,
        cognite_client.entity_matching._get_base_url_with_base_path()
        + cognite_client.entity_matching._RESOURCE_PATH
        + "/",
        status=200,
        json=response_body,
    )
    yield rsps


@pytest.fixture
def mock_status_ok(rsps, cognite_client):
    response_body = {"id": 123, "status": "Completed", "createdTime": 42, "statusTime": 456, "startTime": 789}
    rsps.add(
        rsps.GET,
        re.compile(
            f"{cognite_client.entity_matching._get_base_url_with_base_path()}{cognite_client.entity_matching._RESOURCE_PATH}/\\d+"
        ),
        status=200,
        json=response_body,
    )
    yield rsps


@pytest.fixture
def mock_retrieve(rsps, cognite_client):
    response_body = {
        "items": [{"id": 123, "status": "Completed", "createdTime": 42, "statusTime": 456, "startTime": 789}]
    }
    rsps.add(
        rsps.POST,
        re.compile(
            f"{cognite_client.entity_matching._get_base_url_with_base_path()}{cognite_client.entity_matching._RESOURCE_PATH}/byids"
        ),
        status=200,
        json=response_body,
    )
    yield rsps


@pytest.fixture
def mock_status_failed(rsps, cognite_client):
    response_body = {"id": 123, "status": "Failed", "errorMessage": "error message"}
    rsps.add(
        rsps.GET,
        re.compile(
            f"{cognite_client.entity_matching._get_base_url_with_base_path()}{cognite_client.entity_matching._RESOURCE_PATH}/\\d+"
        ),
        status=200,
        json=response_body,
    )
    yield rsps


@pytest.fixture
def mock_status_rules_ok(rsps, cognite_client):
    response_body = {"jobId": 456, "status": "Completed", "items": [1]}
    rsps.add(
        rsps.GET,
        re.compile(
            f"{cognite_client.entity_matching._get_base_url_with_base_path()}{cognite_client.entity_matching._RESOURCE_PATH}/rules/\\d+"
        ),
        status=200,
        json=response_body,
    )
    yield rsps


class TestEntityMatching:
    def test_fit(self, cognite_client, mock_fit, mock_status_ok):
        entities_from = [{"id": 1, "name": "xx"}]
        entities_to = [{"id": 2, "name": "yy"}]
        model = cognite_client.entity_matching.fit(
            sources=entities_from, targets=entities_to, true_matches=[(1, 2)], feature_type="bigram"
        )
        assert isinstance(model, EntityMatchingModel)
        assert "EntityMatchingModel(id: 123,status: Queued,error: None)" == str(model)
        assert 42 == model.created_time
        model.wait_for_completion()
        assert "Completed" == model.status
        assert 123 == model.id
        assert 42 == model.created_time
        assert 456 == model.status_time
        assert 789 == model.start_time

        n_fit_calls = 0
        n_status_calls = 0
        for call in mock_fit.calls:
            if call.request.method == "POST":
                n_fit_calls += 1
                assert {
                    "sources": entities_from,
                    "targets": entities_to,
                    "trueMatches": [{"sourceId": 1, "targetId": 2}],
                    "featureType": "bigram",
                    "ignoreMissingFields": False,
                    "matchFields": None,
                    "name": None,
                    "description": None,
                    "externalId": None,
                    "classifier": None,
                } == jsgz_load(call.request.body)
            else:
                n_status_calls += 1
                assert "/123" in call.request.url
        assert 1 == n_fit_calls
        assert 1 == n_status_calls

    def test_ml_fit(self, cognite_client, mock_fit, mock_status_ok):
        # fit_ml should produce the same output as fit. Will eventually be removed
        entities_from = [{"id": 1, "name": "xx"}]
        entities_to = [{"id": 2, "name": "yy"}]
        model = cognite_client.entity_matching.fit(
            sources=entities_from, targets=entities_to, true_matches=[(1, 2)], feature_type="bigram"
        )
        assert isinstance(model, EntityMatchingModel)
        assert "EntityMatchingModel(id: 123,status: Queued,error: None)" == str(model)
        assert 42 == model.created_time
        model.wait_for_completion()
        assert "Completed" == model.status
        assert 123 == model.id

    def test_fit_cognite_resource(self, cognite_client, mock_fit):
        entities_from = [TimeSeries(id=1, name="x", metadata={"ka": "va"})]
        entities_to = [Asset(id=1, external_id="abc", name="x")]
        cognite_client.entity_matching.fit(
            sources=entities_from, targets=entities_to, true_matches=[(1, "abc")], feature_type="bigram"
        )
        assert {
            "sources": [{"id": 1, "name": "x", "metadata.ka": "va"}],
            "targets": [entities_to[0].dump(camel_case=True)],
            "trueMatches": [{"sourceId": 1, "targetExternalId": "abc"}],
            "featureType": "bigram",
            "ignoreMissingFields": False,
            "matchFields": None,
            "name": None,
            "description": None,
            "externalId": None,
            "classifier": None,
        } == jsgz_load(mock_fit.calls[0].request.body)

    def test_fit_fails(self, cognite_client, mock_fit, mock_status_failed):
        entities_from = [{"id": 1, "name": "xx"}]
        entities_to = [{"id": 2, "name": "yy"}]
        model = cognite_client.entity_matching.fit(sources=entities_from, targets=entities_to)
        with pytest.raises(ModelFailedException) as exc_info:
            model.wait_for_completion()
        assert exc_info.type is ModelFailedException
        assert 123 == exc_info.value.id
        assert "error message" == exc_info.value.error_message
        assert "EntityMatchingModel 123 failed with error 'error message'" == str(exc_info.value)

    def test_retrieve(self, cognite_client, mock_retrieve):
        model = cognite_client.entity_matching.retrieve(id=123)
        assert isinstance(model, EntityMatchingModel)
        assert "Completed" == model.status
        assert 123 == model.id
