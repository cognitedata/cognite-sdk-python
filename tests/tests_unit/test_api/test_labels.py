import re

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import LabelDefinition, LabelDefinitionFilter, LabelDefinitionList
from tests.utils import jsgz_load

LABELS_API = CogniteClient().labels


@pytest.fixture
def mock_labels_response(rsps):
    response_body = {
        "items": [{"name": "Pump", "description": "guess", "externalId": "PUMP", "createdTime": 1575892259245}]
    }

    url_pattern = re.compile(re.escape(LABELS_API._get_base_url_with_base_path()) + "/.+")
    rsps.assert_all_requests_are_fired = False

    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
    yield rsps


class TestLabels:
    def test_list(self, mock_labels_response):
        res = LABELS_API.list(external_id_prefix="P")
        assert "P" == jsgz_load(mock_labels_response.calls[0].request.body)["filter"]["externalIdPrefix"]
        assert mock_labels_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_call(self, mock_labels_response):
        list(LABELS_API(limit=10))
        calls = mock_labels_response.calls
        assert 1 == len(calls)
        assert {"cursor": None, "limit": 10, "filter": {}} == jsgz_load(calls[0].request.body)

    def test_create_single(self, mock_labels_response):
        res = LABELS_API.create(LabelDefinition(external_id="1"))
        assert isinstance(res, LabelDefinition)
        assert mock_labels_response.calls[0].response.json()["items"][0] == res.dump(camel_case=True)
        assert {"items": [{"externalId": "1"}]} == jsgz_load(mock_labels_response.calls[0].request.body)

    def test_create_multiple(self, mock_labels_response):
        res = LABELS_API.create([LabelDefinition(external_id="1"), LabelDefinition(external_id="2")])
        assert isinstance(res, LabelDefinitionList)
        assert mock_labels_response.calls[0].response.json()["items"] == res.dump(camel_case=True)
        assert {"items": [{"externalId": "1"}, {"externalId": "2"}]} == jsgz_load(
            mock_labels_response.calls[0].request.body
        )

    def test_delete_single(self, mock_labels_response):
        res = LABELS_API.delete(external_id="PUMP")
        assert {"items": [{"externalId": "PUMP"}]} == jsgz_load(mock_labels_response.calls[0].request.body)
        assert res is None

    def test_delete_multiple(self, mock_labels_response):
        res = LABELS_API.delete(external_id=["PUMP", "VALVE"])
        assert {"items": [{"externalId": "PUMP"}, {"externalId": "VALVE"}]} == jsgz_load(
            mock_labels_response.calls[0].request.body
        )
        assert res is None
