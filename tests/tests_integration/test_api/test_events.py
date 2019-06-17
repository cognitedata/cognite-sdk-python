from unittest import mock

import pytest

import cognite.client.utils._time
from cognite.client import CogniteClient, utils
from cognite.client.data_classes import Event, EventFilter, EventUpdate
from cognite.client.exceptions import CogniteAPIError
from tests.utils import set_request_limit

COGNITE_CLIENT = CogniteClient()


@pytest.fixture
def new_event():
    event = COGNITE_CLIENT.events.create(Event())
    yield event
    COGNITE_CLIENT.events.delete(id=event.id)
    assert COGNITE_CLIENT.events.retrieve(event.id) is None


@pytest.fixture
def post_spy():
    with mock.patch.object(COGNITE_CLIENT.events, "_post", wraps=COGNITE_CLIENT.events._post) as _:
        yield


class TestEventsAPI:
    def test_retrieve(self):
        res = COGNITE_CLIENT.events.list(limit=1)
        assert res[0] == COGNITE_CLIENT.events.retrieve(res[0].id)

    def test_retrieve_multiple(self):
        res = COGNITE_CLIENT.events.list(limit=2)
        assert res == COGNITE_CLIENT.events.retrieve_multiple([e.id for e in res])

    def test_list(self, post_spy):
        with set_request_limit(COGNITE_CLIENT.events, 10):
            res = COGNITE_CLIENT.events.list(limit=20)

        assert 20 == len(res)
        assert 2 == COGNITE_CLIENT.events._post.call_count

    def test_search(self):
        res = COGNITE_CLIENT.events.search(
            filter=EventFilter(start_time={"min": cognite.client.utils._time.timestamp_to_ms("2d-ago")})
        )
        assert len(res) > 0

    def test_update(self, new_event):
        update_asset = EventUpdate(new_event.id).metadata.set({"bla": "bla"})
        res = COGNITE_CLIENT.events.update(update_asset)
        assert {"bla": "bla"} == res.metadata
