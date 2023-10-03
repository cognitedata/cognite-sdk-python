"""
This file contains integration tests for the logic in the generic API client. However, since we cannot instantiate a
generic resource, an arbitrary resource is used instead to test the endpoint.
"""
from unittest.mock import patch

import pytest
from pytest import MonkeyPatch

from cognite.client import CogniteClient
from cognite.client.data_classes import Asset, Event, EventFilter, EventList, aggregations, filters
from cognite.client.data_classes.events import EventProperty
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError


@pytest.fixture
def post_spy_event(cognite_client):
    dps_api = cognite_client.events
    with patch.object(dps_api, "_post", wraps=dps_api._post):
        yield


class TestAPIClientUpsert:
    def test_upsert_2_items_one_preexisting(self, cognite_client: CogniteClient) -> None:
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
            res = cognite_client.events.upsert([new_event, preexisting_update], mode="replace")

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
            res = cognite_client.events.upsert(new_event, mode="replace")

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
            res = cognite_client.events.upsert([new_event, existing_update], mode="replace")

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

    def test_upsert_split_into_multiple_tasks(
        self, cognite_client: CogniteClient, monkeypatch: MonkeyPatch, post_spy_event
    ) -> None:
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
            res = cognite_client.events.upsert([new_event, preexisting_update], mode="replace")

            # Assert
            assert cognite_client.events._post.call_count >= 2
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
                cognite_client.events.upsert([new_event, preexisting_update], mode="replace")

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
                cognite_client.events.upsert([new_event, preexisting_update], mode="replace")

            # Assert
            assert e.value.code == 400
            assert len(e.value.successful) == 1
            assert "size must be between 0 and 64" in e.value.message
        finally:
            cognite_client.events.delete(
                external_id=[new_event.external_id, preexisting.external_id], ignore_unknown_ids=True
            )

    def test_upsert_with_invalid_mode(self, cognite_client: CogniteClient):
        # Arrange
        new_event = Event(
            external_id="test_upsert_with_invalid_mode:new",
            type="test__py__sdk",
            start_time=0,
            end_time=1,
            subtype="mySubType1",
        )

        # Act
        try:
            with pytest.raises(ValueError) as e:
                cognite_client.events.upsert(new_event, mode="invalid_mode")

            # Assert
            assert "invalid_mode" in e.value.args[0]
        finally:
            # Just in case the even gets created
            cognite_client.events.delete(external_id=new_event.external_id, ignore_unknown_ids=True)

    def test_upsert_with_invalid_internal_id(self, cognite_client: CogniteClient):
        # Arrange
        new_event = Event(
            # external_id="test_upsert_with_invalid_mode:new",
            id=666,
            type="test__py__sdk",
            start_time=0,
            end_time=1,
            subtype="mySubType1",
        )

        # Act
        try:
            with pytest.raises(CogniteNotFoundError) as e:
                cognite_client.events.upsert(new_event, mode="replace")

            # Assert
            assert [{"id": 666}] == e.value.not_found
        finally:
            # Just in case the even gets created
            cognite_client.events.delete(id=new_event.id, ignore_unknown_ids=True)

    def test_upsert_with_empty_external_id(self, cognite_client: CogniteClient):
        new_asset = Asset(
            external_id="test_upsert_with_empty_external_id:new_asset",
            name="test_upsert_with_empty_external_id",
        )
        existing_asset = Asset(
            external_id="",
            name="test_upsert_with_empty_external_id",
        )
        update_asset = Asset._load(existing_asset.dump(camel_case=True))
        update_asset.name = "test_upsert_with_empty_external_id_updated"

        try:
            created = cognite_client.assets.create(existing_asset)
            assert created.id is not None

            # Act
            updated = cognite_client.assets.upsert([new_asset, update_asset], mode="replace")

            # Assert
            assert updated[1].id == created.id
            assert updated[1].name == update_asset.name
        finally:
            cognite_client.assets.delete(external_id=existing_asset.external_id, ignore_unknown_ids=True)

    def test_upsert_with_patch_option(self, cognite_client: CogniteClient):
        # Arrange
        existing_event = Event(
            external_id="test_upsert_with_patch_option:existing",
            type="mypType1",
            start_time=0,
            end_time=1,
            subtype="mySubType1",
        )
        updated_event = Event(
            external_id=existing_event.external_id,
            subtype="mySubType2",
        )

        try:
            created = cognite_client.events.create(existing_event)
            assert created.id is not None

            # Act
            updated = cognite_client.events.upsert(updated_event, mode="patch")

            # Assert
            assert updated.id == created.id
            assert updated.subtype == updated_event.subtype
            assert updated.type == existing_event.type
            assert updated.start_time == existing_event.start_time
            assert updated.end_time == existing_event.end_time
        finally:
            cognite_client.events.delete(external_id=existing_event.external_id, ignore_unknown_ids=True)


@pytest.fixture(scope="session")
def event_list(cognite_client: CogniteClient) -> EventList:
    prefix = "events:_advanced_aggregate:"
    events = EventList(
        [
            Event(
                external_id=f"{prefix}1",
                type="type1",
                subtype="subtype1",
                start_time=0,
                source="source1",
                metadata={
                    "timezone": "Europe/Oslo",
                    "shop:dynamic": "ignore",
                    "shop:static": "file",
                },
            ),
            Event(
                external_id=f"{prefix}2",
                type="type1",
                subtype="subtype2",
                start_time=100,
                source="source1",
                metadata={"timezone": "Europe/Oslo", "shop:dynamic": "rerun", "shop:static": "config"},
            ),
            Event(
                external_id=f"{prefix}3",
                type="type2",
                subtype="subtype1",
                start_time=200,
                end_time=500,
                source="source2",
                metadata={
                    "timezone": "America/New_York",
                    "shop:dynamic": "taxing",
                    "shop:static": "config",
                },
            ),
        ]
    )
    retrieved_events = cognite_client.events.retrieve_multiple(
        external_ids=events.as_external_ids(), ignore_unknown_ids=True
    )
    if len(retrieved_events) == len(events):
        return retrieved_events
    return cognite_client.events.upsert(events, mode="replace")


class TestAPIClientAdvancedAggregate:
    def test_aggregate_property_with_all_filters(self, cognite_client: CogniteClient, event_list: EventList):
        filter_ = EventFilter(external_id_prefix="_advanced_aggregate:")
        advanced_filter = filters.Not(filters.Equals(EventProperty.source, "source2"))
        agg_filter = aggregations.Prefix("subtype1")

        count = cognite_client.events.aggregate_cardinality_values(
            property=EventProperty.subtype, filter=filter_, advanced_filter=advanced_filter, aggregate_filter=agg_filter
        )

        assert count == sum(
            1
            for event in event_list
            if event.subtype.startswith("type1")
            and event.external_id.startswith("_advanced_aggregate:") == "subtype1"
            and event.source != "source2"
        )

    def test_aggregate_path_with_all_filters(self, cognite_client: CogniteClient, event_list: EventList):
        filter_ = EventFilter(external_id_prefix="_advanced_aggregate:")
        advanced_filter = filters.Not(filters.Equals(EventProperty.type, "type1"))
        agg_filter = aggregations.Prefix("shop")

        count = cognite_client.events.aggregate_cardinality_properties(
            path=EventProperty.metadata, filter=filter_, advanced_filter=advanced_filter, aggregate_filter=agg_filter
        )

        assert count == sum(
            1
            for event in event_list
            if event.external_id.startswith("_advanced_aggregate:") == "subtype1" and event.type != "type1"
            for key in event.metadata
            if key.startswith("shop")
        )
