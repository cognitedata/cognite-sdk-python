import re

import pytest

from cognite.client import CogniteClient
from cognite.client.api.events import Event, EventList, EventUpdate
from tests.utils import jsgz_load

EVENTS_API = CogniteClient().events


@pytest.fixture
def mock_events_response(rsps):
    response_body = {
        "data": {
            "items": [
                {
                    "externalId": "string",
                    "startTime": 0,
                    "endTime": 0,
                    "description": "string",
                    "metadata": {},
                    "assetIds": [1],
                    "source": "string",
                    "id": 1,
                    "lastUpdatedTime": 0,
                }
            ]
        }
    }

    url_pattern = re.compile(re.escape(EVENTS_API._base_url) + "/.+")
    rsps.assert_all_requests_are_fired = False

    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
    yield rsps


class TestEvents:
    def test_get_single(self, mock_events_response):
        res = EVENTS_API.get(id=1)
        assert isinstance(res, Event)
        assert mock_events_response.calls[0].response.json()["data"]["items"][0] == res.dump(camel_case=True)

    def test_get_multiple(self, mock_events_response):
        res = EVENTS_API.get(id=[1])
        assert isinstance(res, EventList)
        assert mock_events_response.calls[0].response.json()["data"]["items"] == res.dump(camel_case=True)

    def test_list(self, mock_events_response):
        res = EVENTS_API.list()
        assert mock_events_response.calls[0].response.json()["data"]["items"] == res.dump(camel_case=True)

    def test_create_single(self, mock_events_response):
        res = EVENTS_API.create(Event(external_id="1"))
        assert isinstance(res, Event)
        assert mock_events_response.calls[0].response.json()["data"]["items"][0] == res.dump(camel_case=True)

    def test_create_multiple(self, mock_events_response):
        res = EVENTS_API.create([Event(external_id="1")])
        assert isinstance(res, EventList)
        assert mock_events_response.calls[0].response.json()["data"]["items"] == res.dump(camel_case=True)

    def test_iter_single(self, mock_events_response):
        for event in EVENTS_API:
            assert mock_events_response.calls[0].response.json()["data"]["items"][0] == event.dump(camel_case=True)

    def test_iter_chunk(self, mock_events_response):
        for events in EVENTS_API(chunk_size=1):
            assert mock_events_response.calls[0].response.json()["data"]["items"] == events.dump(camel_case=True)

    def test_delete_single(self, mock_events_response):
        res = EVENTS_API.delete(id=1)
        assert {"items": [{"id": 1}]} == jsgz_load(mock_events_response.calls[0].request.body)
        assert res is None

    def test_delete_multiple(self, mock_events_response):
        res = EVENTS_API.delete(id=[1])
        assert {"items": [{"id": 1}]} == jsgz_load(mock_events_response.calls[0].request.body)
        assert res is None

    def test_update_with_resource_class(self, mock_events_response):
        res = EVENTS_API.update(Event(id=1))
        assert isinstance(res, Event)
        assert mock_events_response.calls[0].response.json()["data"]["items"][0] == res.dump(camel_case=True)

    def test_update_with_update_class(self, mock_events_response):
        res = EVENTS_API.update(EventUpdate(id=1).description_set("blabla"))
        assert isinstance(res, Event)
        assert mock_events_response.calls[0].response.json()["data"]["items"][0] == res.dump(camel_case=True)

    def test_update_multiple(self, mock_events_response):
        res = EVENTS_API.update([EventUpdate(id=1).description_set("blabla")])
        assert isinstance(res, EventList)
        assert mock_events_response.calls[0].response.json()["data"]["items"] == res.dump(camel_case=True)

    def test_search(self, mock_events_response):
        res = EVENTS_API.search()
        assert mock_events_response.calls[0].response.json()["data"]["items"] == res.dump(camel_case=True)
