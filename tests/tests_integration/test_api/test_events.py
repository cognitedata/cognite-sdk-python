from __future__ import annotations

from datetime import datetime
from unittest import mock

import pytest

from cognite.client import CogniteClient, utils
from cognite.client.data_classes import EndTimeFilter, Event, EventFilter, EventList, EventUpdate, filters
from cognite.client.data_classes.events import EventProperty, EventWrite, SortableEventProperty
from cognite.client.exceptions import CogniteNotFoundError
from cognite.client.utils import timestamp_to_ms
from cognite.client.utils._text import random_string
from tests.utils import set_request_limit


@pytest.fixture
def new_event(cognite_client):
    event = cognite_client.events.create(Event(type="test__py__sdk"))
    yield event
    cognite_client.events.delete(id=event.id)
    assert cognite_client.events.retrieve(event.id) is None


@pytest.fixture
def event_list(cognite_client: CogniteClient) -> EventList:
    prefix = "integration_test:"
    events = EventList(
        [
            Event(
                external_id=f"{prefix}event1_lorem_ipsum",
                description="This is a a test event with some lorem ipsum text.",
                start_time=timestamp_to_ms(datetime(2023, 8, 9, 11, 42)),
                metadata={
                    "timezone": "Europe/Oslo",
                },
            ),
            Event(
                external_id=f"{prefix}event2",
                description="This is also a test event, this time without the same text as the other one.",
                end_time=timestamp_to_ms(datetime(2023, 8, 9, 11, 43)),
                type="lorem ipsum",
                metadata={
                    "timezone": "America/New_York",
                    "some_other_key": "some_other_value",
                },
            ),
        ]
    )
    retrieved = cognite_client.events.retrieve_multiple(external_ids=events.as_external_ids(), ignore_unknown_ids=True)
    if len(retrieved) == len(events):
        return retrieved
    return cognite_client.events.upsert(events, mode="replace")


@pytest.fixture
def post_spy(cognite_client):
    with mock.patch.object(cognite_client.events, "_post", wraps=cognite_client.events._post) as _:
        yield


@pytest.fixture
def root_test_asset(cognite_client):
    for asset in cognite_client.assets(root=True):
        if asset.name.startswith("test__"):
            return asset


@pytest.fixture(scope="session")
def twenty_events(cognite_client: CogniteClient) -> EventList:
    events = [
        EventWrite(
            external_id=f"twenty_events_{i}",
            type="test-data-populator",
            start_time=utils.timestamp_to_ms(timestamp_to_ms(datetime(2023, 8, 9, 11, 42))),
            end_time=utils.timestamp_to_ms(timestamp_to_ms(datetime(2023, 8, 9, 11, 43))),
        )
        for i in range(20)
    ]
    return cognite_client.events.upsert(events, mode="replace")


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

    @pytest.mark.usefixtures("twenty_events")
    def test_list(self, cognite_client, post_spy):
        with set_request_limit(cognite_client.events, 10):
            res = cognite_client.events.list(limit=20)

        assert 20 == len(res)
        assert 2 == cognite_client.events._post.call_count

    def test_list_ongoing(self, cognite_client):
        res = cognite_client.events.list(end_time=EndTimeFilter(is_null=True), limit=10)
        assert len(res) > 0

    def test_aggregation(self, cognite_client, new_event, twenty_events: EventList):
        res_aggregate = cognite_client.events.aggregate(filter=EventFilter(type=twenty_events[0].type))
        assert res_aggregate[0].count > 0

    def test_partitioned_list(self, cognite_client, post_spy, twenty_events: EventList):
        # stop race conditions by cutting off max created time
        type = twenty_events[0].type
        maxtime = twenty_events[0].start_time + 1
        res_flat = cognite_client.events.list(limit=None, type=type, start_time={"max": maxtime})
        res_part = cognite_client.events.list(partitions=8, type=type, start_time={"max": maxtime}, limit=None)
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
        res = cognite_client.events.search(filter=EventFilter(start_time={"min": 1691574120000}))
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
        new_event = Event(
            external_id="test_upsert2_one_preexisting:new" + random_string(5),
            type="test__py__sdk",
            start_time=0,
            end_time=1,
            subtype="mySubType1",
        )
        preexisting = Event(
            external_id="test_upsert2_one_preexisting:preexisting" + random_string(5),
            type="test__py__sdk",
            start_time=0,
            end_time=1,
            subtype="mySubType2",
        )
        preexisting_update = Event.load(preexisting.dump(camel_case=True))
        preexisting_update.subtype = "mySubType1"

        try:
            created_existing = cognite_client.events.create(preexisting)
            assert created_existing is not None

            res = cognite_client.events.upsert([new_event, preexisting_update], mode="replace")

            assert len(res) == 2
            assert new_event.external_id == res[0].external_id
            assert preexisting.external_id == res[1].external_id
            assert new_event.subtype == res[0].subtype
            assert preexisting_update.subtype == res[1].subtype
        finally:
            cognite_client.events.delete(
                external_id=[new_event.external_id, preexisting.external_id], ignore_unknown_ids=True
            )

    def test_filter_search(self, cognite_client: CogniteClient, event_list: EventList) -> None:
        f = filters
        is_integration_test = f.Prefix(EventProperty.external_id, "integration_test:")
        has_lorem_ipsum = f.Search(EventProperty.description, "lorem ipsum")

        result = cognite_client.events.filter(
            f.And(is_integration_test, has_lorem_ipsum), sort=SortableEventProperty.external_id
        )
        assert len(result) == 1, "Expected only one event to match the filter"
        assert result[0].external_id == "integration_test:event1_lorem_ipsum"

    def test_filter_search_without_sort(self, cognite_client: CogniteClient, event_list: EventList) -> None:
        f = filters
        is_integration_test = f.Prefix(EventProperty.external_id, "integration_test:")
        has_lorem_ipsum = f.Search(EventProperty.description, "lorem ipsum")

        result = cognite_client.events.filter(f.And(is_integration_test, has_lorem_ipsum), sort=None)
        assert len(result) == 1, "Expected only one event to match the filter"
        assert result[0].external_id == "integration_test:event1_lorem_ipsum"

    def test_list_with_advanced_filter(self, cognite_client: CogniteClient, event_list: EventList) -> None:
        f = filters
        has_lorem_ipsum = f.Search(EventProperty.description, "lorem ipsum")

        result = cognite_client.events.list(
            external_id_prefix="integration_test:",
            advanced_filter=has_lorem_ipsum,
            sort=SortableEventProperty.external_id,
        )
        assert len(result) == 1, "Expected only one event to match the filter"
        assert result[0].external_id == "integration_test:event1_lorem_ipsum"

    def test_aggregate_count(self, cognite_client: CogniteClient, event_list: EventList) -> None:
        f = filters
        is_integration_test = f.Prefix(EventProperty.external_id, "integration_test:")

        count = cognite_client.events.aggregate_count(advanced_filter=is_integration_test)
        assert count >= len(event_list)

    def test_aggregate_has_type(self, cognite_client: CogniteClient, event_list: EventList) -> None:
        f = filters
        is_integration_test = f.Prefix(EventProperty.external_id, "integration_test:")

        count = cognite_client.events.aggregate_count(EventProperty.type, advanced_filter=is_integration_test)
        assert count >= len([e for e in event_list if e.type])

    def test_aggregate_type_count(self, cognite_client: CogniteClient, event_list: EventList) -> None:
        f = filters
        is_integration_test = f.Prefix(EventProperty.external_id, "integration_test:")

        count = cognite_client.events.aggregate_cardinality_values(
            EventProperty.type, advanced_filter=is_integration_test
        )
        assert count >= len({e.type for e in event_list if e.type})

    def test_aggregate_metadata_keys_count(self, cognite_client: CogniteClient, event_list: EventList) -> None:
        f = filters
        is_integration_test = f.Prefix(EventProperty.external_id, "integration_test:")

        count = cognite_client.events.aggregate_cardinality_properties(
            EventProperty.metadata, advanced_filter=is_integration_test
        )
        assert count >= len({k for e in event_list for k in e.metadata})

    def test_aggregate_unique_types(self, cognite_client: CogniteClient, event_list: EventList) -> None:
        f = filters
        is_integration_test = f.Prefix(EventProperty.external_id, "integration_test:")

        result = cognite_client.events.aggregate_unique_values(
            property=EventProperty.type, advanced_filter=is_integration_test
        )
        assert result
        assert set(result.unique) >= {e.type for e in event_list if e.type}

    def test_aggregate_unique_metadata_keys(self, cognite_client: CogniteClient, event_list: EventList) -> None:
        f = filters
        is_integration_test = f.Prefix(EventProperty.external_id, "integration_test:")

        result = cognite_client.events.aggregate_unique_properties(
            EventProperty.metadata, advanced_filter=is_integration_test
        )
        assert result
        assert {tuple(item.value["property"]) for item in result} >= {
            ("metadata", key.casefold()) for a in event_list for key in a.metadata or []
        }
