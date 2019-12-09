import re

import pytest

from cognite.client.data_classes import Type, TypeFilter, TypeList
from cognite.client.experimental import CogniteClient
from tests.utils import jsgz_load

TYPES_API = CogniteClient().types


@pytest.fixture
def mock_types_response(rsps):
    response_body = {
        "items": [
            {
                "externalId": "456",
                "properties": [{"propertyId": "abc", "name": "123", "type": "string"}],
                "id": 1,
                "version": 4,
                "createdTime": 1575892259245,
                "lastUpdatedTime": 1575892259245,
            }
        ]
    }

    url_pattern = re.compile(re.escape(TYPES_API._get_base_url_with_base_path()) + "/.+")
    rsps.assert_all_requests_are_fired = False

    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
    yield rsps


class TestTypes:
    def test_retrieve_single(self, mock_types_response):
        res = TYPES_API.retrieve(id=1)
        assert isinstance(res, Type)
        assert mock_types_response.calls[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_retrieve_multiple(self, mock_types_response):
        res = TYPES_API.retrieve_multiple(ids=[1])
        assert isinstance(res, TypeList)
        assert mock_types_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_list(self, mock_types_response):
        res = TYPES_API.list(external_id_prefix="4")
        assert "4" == jsgz_load(mock_types_response.calls[0].request.body)["filter"]["externalIdPrefix"]
        assert mock_types_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_call(self, mock_types_response):
        list(TYPES_API(limit=10))
        calls = mock_types_response.calls
        assert 1 == len(calls)
        assert {"cursor": None, "limit": 10, "filter": {}} == jsgz_load(calls[0].request.body)

    def test_create_single(self, mock_types_response):
        res = TYPES_API.create(Type(external_id="1", properties=[]))
        assert isinstance(res, Type)
        assert mock_types_response.calls[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_create_multiple(self, mock_types_response):
        res = TYPES_API.create([Type(external_id="1", properties=[])])
        assert isinstance(res, TypeList)
        assert mock_types_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_delete_single(self, mock_types_response):
        res = TYPES_API.delete(id=1)
        assert {"items": [{"id": 1}]} == jsgz_load(mock_types_response.calls[0].request.body)
        assert res is None

    def test_delete_multiple(self, mock_types_response):
        res = TYPES_API.delete(id=[1])
        assert {"items": [{"id": 1}]} == jsgz_load(mock_types_response.calls[0].request.body)
        assert res is None

    def test_update(self, mock_types_response):
        res = TYPES_API.update(
            external_id="456", version=4, new_version=Type(external_id="456", version=4, properties=[])
        )
        assert isinstance(res, Type)
        assert mock_types_response.calls[0].response.json()["items"][0] == res.dump(camel_case=True)
