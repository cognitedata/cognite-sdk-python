import pytest

from cognite.client import CogniteClient, utils
from cognite.client.data_classes import Event, EventFilter, EventUpdate
from cognite.client.exceptions import CogniteAPIError
from tests.utils import set_request_limit

COGNITE_CLIENT = CogniteClient(debug=True)


@pytest.fixture
def new_event():
    event = COGNITE_CLIENT.events.create(Event())
    yield event
    COGNITE_CLIENT.events.delete(id=event.id)
    with pytest.raises(CogniteAPIError) as e:
        COGNITE_CLIENT.events.retrieve(event.id)
    assert 400 == e.value.code


class TestEventsAPI:
    def test_retrieve(self):
        res = COGNITE_CLIENT.events.list(limit=1)
        assert res[0] == COGNITE_CLIENT.events.retrieve(res[0].id)

    @pytest.mark.xfail(strict=True)
    def test_list(self, mocker):
        mocker.spy(COGNITE_CLIENT.events, "_post")

        with set_request_limit(COGNITE_CLIENT.events, 10):
            res = COGNITE_CLIENT.events.list(limit=20)

        assert 20 == len(res)
        assert 2 == COGNITE_CLIENT.events._post.call_count

    @pytest.mark.xfail(strict=True)
    def test_search(self):
        res = COGNITE_CLIENT.events.search(filter=EventFilter(start_time={"min": utils.timestamp_to_ms("2d-ago")}))
        assert len(res) > 0

    @pytest.mark.xfail(strict=True)
    def test_update(self, new_event):
        update_asset = EventUpdate(new_event.id).metadata.set({"bla": "bla"})
        res = COGNITE_CLIENT.events.update(update_asset)
        assert {"bla": "bla"} == res.metadata
