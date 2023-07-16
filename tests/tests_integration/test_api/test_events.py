from __future__ import annotations

from datetime import datetime
from unittest import mock

import pytest
from _pytest.monkeypatch import MonkeyPatch

import cognite.client.utils._time
from cognite.client import CogniteClient, utils
from cognite.client.data_classes import EndTimeFilter, Event, EventFilter, EventUpdate
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError
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
        res_listed_ids = [e.id for e in cognite_client.events.list(limit=2, type="test-data-populator")]
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

    def test_upsert_2_events_one_preexisting(self, cognite_client: CogniteClient) -> None:
        # Arrange
        new_event = Event(
            external_id="test_upsert2_one_preexisting:new",
            type="test__py__sdk",
            start_time=0,
            end_time=1,
            subtype="mySubType1",
        )
        preexisting = Event(
            external_id="test_upsert2_one_preexisting:preexisting",
            type="test__py__sdk",
            start_time=0,
            end_time=1,
            subtype="mySubType2",
        )
        preexisting_update = Event._load(preexisting.dump(camel_case=True))
        preexisting_update.subtype = "mySubType1"

        try:
            created_existing = cognite_client.events.create(preexisting)
            assert created_existing is not None

            # Act
            res = cognite_client.events.upsert([new_event, preexisting_update])

            # Assert
            assert len(res) == 2
            assert new_event.external_id == res[0].external_id
            assert preexisting.external_id == res[1].external_id
            assert new_event.subtype == res[0].subtype
            assert preexisting_update.subtype == res[1].subtype
        finally:
            cognite_client.events.delete(
                external_id=[new_event.external_id, preexisting.external_id], ignore_unknown_ids=True
            )

    def test_upsert_with_all_preexisting(self, cognite_client: CogniteClient) -> None:
        # Arrange
        new_event = Event(
            external_id="test_upsert_all_preexisting:new",
            type="test__py__sdk",
            start_time=0,
            end_time=1,
            subtype="mySubType1",
        )

        try:
            _ = cognite_client.events.create(new_event)

            # Act
            res = cognite_client.events.upsert(new_event)

            # Assert
            assert isinstance(res, Event)
            assert new_event.external_id == res.external_id
        finally:
            cognite_client.events.delete(external_id=new_event.external_id, ignore_unknown_ids=True)

    def test_upsert_without_external_id(self, cognite_client: CogniteClient) -> None:
        # Arrange
        new_event = Event(
            external_id="test_upsert_without_external_id:new",
            type="test__py__sdk",
            start_time=0,
            end_time=1,
            subtype="mySubType1",
        )
        existing = Event(
            external_id="test_upsert_without_external_id:existing",
            type="test__py__sdk",
            start_time=0,
            end_time=1,
            subtype="mySubType2",
        )
        existing_update = Event._load(existing.dump(camel_case=True))
        existing_update.subtype = "mySubType1"

        try:
            created = cognite_client.events.create(existing)
            existing_update.external_id = None
            existing_update.id = created.id

            # Act
            res = cognite_client.events.upsert([new_event, existing_update])

            # Assert
            assert len(res) == 2
            assert new_event.external_id == res[0].external_id
            assert existing.external_id == res[1].external_id
            assert new_event.subtype == res[0].subtype
            assert existing_update.subtype == res[1].subtype
        finally:
            cognite_client.events.delete(
                external_id=[new_event.external_id, existing.external_id], ignore_unknown_ids=True
            )

    def test_upsert_split_into_multiple_tasks(self, cognite_client: CogniteClient, monkeypatch: MonkeyPatch) -> None:
        # Arrange
        new_event = Event(
            external_id="test_upsert_split_into_multiple_tasks:new",
            type="test__py__sdk",
            start_time=0,
            end_time=1,
            subtype="mySubType1",
        )
        preexisting = Event(
            external_id="test_upsert_split_into_multiple_tasks:preexisting",
            type="test__py__sdk",
            start_time=0,
            end_time=1,
            subtype="mySubType2",
        )
        preexisting_update = Event._load(preexisting.dump(camel_case=True))
        preexisting_update.subtype = "mySubType1"

        try:
            created_existing = cognite_client.events.create(preexisting)
            assert created_existing is not None
            monkeypatch.setattr(cognite_client.events, "_UPDATE_LIMIT", 1)

            # Act
            res = cognite_client.events.upsert([new_event, preexisting_update])

            # Assert
            assert len(res) == 2
            assert new_event.external_id == res[0].external_id
            assert preexisting.external_id == res[1].external_id
            assert new_event.subtype == res[0].subtype
            assert preexisting_update.subtype == res[1].subtype
        finally:
            cognite_client.events.delete(
                external_id=[new_event.external_id, preexisting.external_id], ignore_unknown_ids=True
            )

    def test_upsert_invalid_update(self, cognite_client: CogniteClient, monkeypatch: MonkeyPatch) -> None:
        # Arrange
        new_event = Event(
            external_id="test_upsert_invalid_update:new",
            type="test__py__sdk",
            start_time=0,
            end_time=1,
            subtype="mySubType1",
        )
        preexisting = Event(
            external_id="test_upsert_invalid_update:preexisting",
            type="test__py__sdk",
            start_time=0,
            end_time=1,
            subtype="mySubType2",
        )
        preexisting_update = Event._load(preexisting.dump(camel_case=True))
        preexisting_update.type = "invalid_length" * 64

        try:
            created = cognite_client.events.create(preexisting)
            assert created
            monkeypatch.setattr(cognite_client.events, "_UPDATE_LIMIT", 1)

            # Act
            with pytest.raises(CogniteAPIError) as e:
                cognite_client.events.upsert([new_event, preexisting_update])

            # Assert
            assert e.value.code == 400
            # The first update call should fail.
            assert len(e.value.failed) == 2
            assert "size must be between 0 and 64" in e.value.message
        finally:
            cognite_client.events.delete(
                external_id=[new_event.external_id, preexisting.external_id], ignore_unknown_ids=True
            )

    def test_upsert_invalid_create(self, cognite_client: CogniteClient, monkeypatch: MonkeyPatch) -> None:
        # Arrange
        new_event = Event(
            external_id="test_upsert_invalid_create:new",
            type="test__py__sdk",
            start_time=0,
            end_time=1,
            subtype="InvalidLength" * 100,
        )
        preexisting = Event(
            external_id="test_upsert_invalid_create:preexisting",
            type="test__py__sdk",
            start_time=0,
            end_time=1,
            subtype="mySubType2",
        )
        preexisting_update = Event._load(preexisting.dump(camel_case=True))
        preexisting_update.type = "mySubType42"

        try:
            created = cognite_client.events.create(preexisting)
            assert created
            monkeypatch.setattr(cognite_client.events, "_UPDATE_LIMIT", 1)

            # Act
            with pytest.raises(CogniteAPIError) as e:
                cognite_client.events.upsert([new_event, preexisting_update])

            # Assert
            assert e.value.code == 400
            assert len(e.value.successful) == 1
            assert "size must be between 0 and 64" in e.value.message
        finally:
            cognite_client.events.delete(
                external_id=[new_event.external_id, preexisting.external_id], ignore_unknown_ids=True
            )
