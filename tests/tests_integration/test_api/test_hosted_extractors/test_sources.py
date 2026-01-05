from __future__ import annotations

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.hosted_extractors import (
    EventHubSource,
    EventHubSourceUpdate,
    EventHubSourceWrite,
    MQTT5Source,
    MQTT5SourceWrite,
    SourceList,
)
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._text import random_string
from tests.utils import get_or_raise


@pytest.fixture(scope="session")
def one_event_hub_source(cognite_client: CogniteClient, os_and_py_version: str) -> SourceList:
    my_hub = EventHubSourceWrite(
        external_id=f"myNewHub-{os_and_py_version}",
        host="myHost",
        key_name="myKeyName",
        key_value="myKey",
        event_hub_name="myEventHub",
    )
    retrieved = cognite_client.hosted_extractors.sources.retrieve([my_hub.external_id], ignore_unknown_ids=True)
    if retrieved:
        return retrieved
    return cognite_client.hosted_extractors.sources.create([my_hub])


class TestSources:
    def test_create_update_retrieve_delete(self, cognite_client: CogniteClient) -> None:
        my_hub = EventHubSourceWrite(
            external_id=f"myNewHub-{random_string(10)}",
            host="myHost",
            key_name="myKeyName",
            key_value="myKey",
            event_hub_name="myEventHub",
        )
        created = cognite_client.hosted_extractors.sources.create(my_hub)
        try:
            assert isinstance(created, EventHubSource)

            update = EventHubSourceUpdate(external_id=my_hub.external_id).event_hub_name.set("myNewEventHub")
            updated = cognite_client.hosted_extractors.sources.update(update)
            assert isinstance(updated, EventHubSource)
            assert updated.event_hub_name == "myNewEventHub"

            retrieved = cognite_client.hosted_extractors.sources.retrieve(created.external_id)
            assert retrieved is not None
            assert isinstance(retrieved, EventHubSource)
            assert retrieved.external_id == created.external_id
            assert retrieved.event_hub_name == "myNewEventHub"

            cognite_client.hosted_extractors.sources.delete(created.external_id)

            with pytest.raises(CogniteAPIError):
                cognite_client.hosted_extractors.sources.retrieve(created.external_id)

            cognite_client.hosted_extractors.sources.retrieve(created.external_id, ignore_unknown_ids=True)

        finally:
            cognite_client.hosted_extractors.sources.delete(created.external_id, ignore_unknown_ids=True)

    def test_create_update_replace_retrieve(self, cognite_client: CogniteClient) -> None:
        original = MQTT5SourceWrite(
            external_id=f"myMqttSource-{random_string(10)}",
            host="mqtt.hsl.fi",
            port=1883,
        )

        created = cognite_client.hosted_extractors.sources.create(original)
        try:
            update = MQTT5SourceWrite(original.external_id, host="mqtt.hsl.fi", port=1884)

            updated = cognite_client.hosted_extractors.sources.update(update, mode="replace")
            assert isinstance(updated, MQTT5Source)
            assert updated.port == 1884

            retrieved = cognite_client.hosted_extractors.sources.retrieve(created.external_id)

            assert retrieved is not None
            assert isinstance(retrieved, MQTT5Source)
            assert retrieved.external_id == created.external_id
            assert retrieved.port == 1884
        finally:
            cognite_client.hosted_extractors.sources.delete(created.external_id, ignore_unknown_ids=True)

    @pytest.mark.usefixtures("one_event_hub_source")
    def test_list(self, cognite_client: CogniteClient) -> None:
        res = cognite_client.hosted_extractors.sources.list(limit=1)
        assert len(res) == 1
        assert isinstance(res, SourceList)

    def test_update_using_write_object(self, cognite_client: CogniteClient) -> None:
        my_hub = EventHubSourceWrite(
            external_id=f"to-update-{random_string(10)}",
            host="myHost",
            key_name="myKeyName",
            key_value="myKey",
            event_hub_name="myEventHub",
        )
        created = cognite_client.hosted_extractors.sources.create(my_hub)
        try:
            my_new_hub = EventHubSourceWrite(
                external_id=get_or_raise(created.external_id),
                host="updatedHost",
                key_name="updatedKeyName",
                key_value="updatedKey",
                event_hub_name="updatedEventHub",
            )

            updated = cognite_client.hosted_extractors.sources.update(my_new_hub)
            assert isinstance(updated, EventHubSource)
            assert updated.host == my_new_hub.host
        finally:
            cognite_client.hosted_extractors.sources.delete(created.external_id, ignore_unknown_ids=True)
