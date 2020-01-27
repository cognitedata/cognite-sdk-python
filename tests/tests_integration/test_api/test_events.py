from datetime import datetime
from unittest import mock

import pytest

import cognite.client.utils._time
from cognite.client import CogniteClient, utils
from cognite.client.data_classes import Event, EventFilter, EventUpdate
from cognite.client.exceptions import CogniteNotFoundError
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


@pytest.fixture
def root_test_asset():
    for asset in COGNITE_CLIENT.assets(root=True):
        if asset.name.startswith("test__"):
            return asset


class TestEventsAPI:
    def test_retrieve(self):
        res = COGNITE_CLIENT.events.list(limit=1)
        assert res[0] == COGNITE_CLIENT.events.retrieve(res[0].id)

    def test_retrieve_multiple(self, root_test_asset):
        res_listed_ids = [
            e.id for e in COGNITE_CLIENT.events.list(limit=2, root_asset_ids=[{"id": root_test_asset.id}])
        ]
        res_lookup_ids = [e.id for e in COGNITE_CLIENT.events.retrieve_multiple(res_listed_ids)]
        for listed_id in res_listed_ids:
            assert listed_id in res_lookup_ids

    def test_retrieve_unknown(self):
        res = COGNITE_CLIENT.events.list(limit=1)
        with pytest.raises(CogniteNotFoundError):
            COGNITE_CLIENT.events.retrieve_multiple(ids=[res[0].id], external_ids=["this does not exist"])
        retr = COGNITE_CLIENT.events.retrieve_multiple(
            ids=[res[0].id], external_ids=["this does not exist"], ignore_unknown_ids=True
        )
        assert 1 == len(retr)

    def test_list(self, post_spy):
        with set_request_limit(COGNITE_CLIENT.events, 10):
            res = COGNITE_CLIENT.events.list(limit=20)

        assert 20 == len(res)
        assert 2 == COGNITE_CLIENT.events._post.call_count

    def test_partitioned_list(self):
        # stop race conditions by cutting off max created time
        maxtime = utils.timestamp_to_ms(datetime(2019, 6, 20, 17, 30))
        res_flat = COGNITE_CLIENT.events.list(limit=None, type="test-data-populator", start_time={"max": maxtime})
        res_part = COGNITE_CLIENT.events.list(
            partitions=8, type="test-data-populator", start_time={"max": maxtime}, limit=None
        )
        assert len(res_flat) > 0
        assert len(res_flat) == len(res_part)
        assert {a.id for a in res_flat} == {a.id for a in res_part}

    def test_assetid_list(self):
        res = COGNITE_CLIENT.events.list(
            limit=None, type="test-data-populator", asset_external_ids=["a", "b"], asset_ids=[0, 1]
        )
        assert 0 == len(res)

    def test_search(self):
        res = COGNITE_CLIENT.events.search(
            filter=EventFilter(start_time={"min": cognite.client.utils._time.timestamp_to_ms("2d-ago")})
        )
        assert len(res) > 0

    def test_update(self, new_event):
        update_asset = EventUpdate(new_event.id).metadata.set({"bla": "bla"})
        res = COGNITE_CLIENT.events.update(update_asset)
        assert {"bla": "bla"} == res.metadata

    def test_delete_with_nonexisting(self):
        a = COGNITE_CLIENT.events.create(Event())
        COGNITE_CLIENT.events.delete(id=a.id, external_id="this event does not exist", ignore_unknown_ids=True)
        assert COGNITE_CLIENT.events.retrieve(id=a.id) is None
