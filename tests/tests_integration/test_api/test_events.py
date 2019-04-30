import pytest

from cognite.client import CogniteClient, utils
from cognite.client.data_classes import Event, EventFilter, EventUpdate
from cognite.client.exceptions import CogniteAPIError

COGNITE_CLIENT = CogniteClient(debug=True)


@pytest.fixture(autouse=True, scope="module")
def set_limit():
    limit_tmp = COGNITE_CLIENT.events._LIMIT
    COGNITE_CLIENT.events._LIMIT = 10
    yield set_limit
    COGNITE_CLIENT.events._LIMIT = limit_tmp


@pytest.fixture
def new_event():
    event = COGNITE_CLIENT.events.create(Event())
    yield event
    COGNITE_CLIENT.events.delete(id=event.id)
    with pytest.raises(CogniteAPIError) as e:
        COGNITE_CLIENT.events.get(event.id)
    assert 400 == e.value.code


class TestEventsAPI:
    def test_get(self):
        res = COGNITE_CLIENT.events.list(limit=1)
        assert res[0] == COGNITE_CLIENT.events.get(res[0].id)

    def test_list(self, mocker):
        mocker.spy(COGNITE_CLIENT.events, "_post")

        res = COGNITE_CLIENT.events.list(limit=20)

        assert 20 == len(res)
        assert 2 == COGNITE_CLIENT.events._post.call_count

    def test_search(self):
        res = COGNITE_CLIENT.events.search(filter=EventFilter(start_time={"min": utils.timestamp_to_ms("2d-ago")}))
        assert len(res) > 0

    def test_update(self, new_event):
        update_asset = EventUpdate(new_event.id).metadata.set({"bla": "bla"})
        res = COGNITE_CLIENT.events.update(update_asset)
        assert {"bla": "bla"} == res.metadata
