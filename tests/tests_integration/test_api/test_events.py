from datetime import datetime
from unittest import mock

import pytest

import cognite.client.utils._time
from cognite.client import utils
from cognite.client.data_classes import EndTimeFilter, Event, EventFilter, EventUpdate
from cognite.client.exceptions import CogniteNotFoundError
from tests.utils import set_request_limit


@pytest.fixture
def new_event(cognite_client):
    event = cognite_client.events.create(Event(type="test__py__sdk"))
    yield event
    cognite_client.events.delete(id=event.id)
    assert cognite_client.events.retrieve(event.id) is None


@pytest.fixture
def post_spy(cognite_client):
    with mock.patch.object(cognite_client.events, "_post", wraps=cognite_client.events._post) as _:
        yield


@pytest.fixture
def root_test_asset(cognite_client):
    for asset in cognite_client.assets(root=True):
        if asset.name.startswith("test__"):
            return asset


class TestEventsAPI:
    def test_retrieve(self, cognite_client):
        res = cognite_client.events.list(limit=1)
        assert res[0] == cognite_client.events.retrieve(res[0].id)

    def test_retrieve_multiple(self, cognite_client, root_test_asset):
        res_listed_ids = [
            e.id for e in cognite_client.events.list(limit=2, root_asset_ids=[{"id": root_test_asset.id}])
        ]
        res_lookup_ids = [e.id for e in cognite_client.events.retrieve_multiple(res_listed_ids)]
        for listed_id in res_listed_ids:
            assert listed_id in res_lookup_ids

    def test_retrieve_unknown(self, cognite_client):
        res = cognite_client.events.list(limit=1)
        with pytest.raises(CogniteNotFoundError):
            cognite_client.events.retrieve_multiple(ids=[res[0].id], external_ids=["this does not exist"])
        retr = cognite_client.events.retrieve_multiple(
            ids=[res[0].id], external_ids=["this does not exist"], ignore_unknown_ids=True
        )
        assert 1 == len(retr)

    def test_list(self, cognite_client, post_spy):
        with set_request_limit(cognite_client.events, 10):
            res = cognite_client.events.list(limit=20)

        assert 20 == len(res)
        assert 2 == cognite_client.events._post.call_count

    def test_list_ongoing(self, cognite_client):
        res = cognite_client.events.list(end_time=EndTimeFilter(is_null=True), limit=10)

        assert len(res) > 0

    def test_aggregation(self, cognite_client, new_event):
        res_aggregate = cognite_client.events.aggregate(filter=EventFilter(type="test__py__sdk"))
        assert res_aggregate[0].count > 0

    def test_partitioned_list(self, cognite_client):
        # stop race conditions by cutting off max created time
        maxtime = utils.timestamp_to_ms(datetime(2019, 5, 25, 17, 30))
        res_flat = cognite_client.events.list(limit=None, type="test-data-populator", start_time={"max": maxtime})
        res_part = cognite_client.events.list(
            partitions=8, type="test-data-populator", start_time={"max": maxtime}, limit=None
        )
        assert len(res_flat) > 0
        assert len(res_flat) == len(res_part)
        assert {a.id for a in res_flat} == {a.id for a in res_part}

    def test_compare_partitioned_gen_and_list(self, cognite_client, post_spy):
        # stop race conditions by cutting off max created time
        maxtime = utils.timestamp_to_ms(datetime(2019, 5, 25, 17, 30))
        res_generator = cognite_client.events(partitions=8, limit=None, created_time={"max": maxtime})
        res_list = cognite_client.events.list(partitions=8, limit=None, created_time={"max": maxtime})
        assert {a.id for a in res_generator} == {a.id for a in res_list}

    def test_assetid_list(self, cognite_client):
        res = cognite_client.events.list(
            limit=None, type="test-data-populator", asset_external_ids=["a", "b"], asset_ids=[0, 1]
        )
        assert 0 == len(res)

    def test_search(self, cognite_client):
        res = cognite_client.events.search(
            filter=EventFilter(start_time={"min": cognite.client.utils._time.timestamp_to_ms("2d-ago")})
        )
        assert len(res) > 0

    def test_update(self, cognite_client, new_event):
        update_event = EventUpdate(new_event.id).metadata.set({"bla": "bla"})
        res = cognite_client.events.update(update_event)
        assert {"bla": "bla"} == res.metadata
        update_event2 = EventUpdate(new_event.id).metadata.set(None)
        res2 = cognite_client.events.update(update_event2)
        assert res2.metadata == {}

    def test_delete_with_nonexisting(self, cognite_client):
        a = cognite_client.events.create(Event())
        cognite_client.events.delete(id=a.id, external_id="this event does not exist", ignore_unknown_ids=True)
        assert cognite_client.events.retrieve(id=a.id) is None
