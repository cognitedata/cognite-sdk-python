from __future__ import annotations

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.hosted_extractors import (
    EventHubSource,
    EventHubSourceUpdate,
    EventHubSourceWrite,
    SourceList,
)
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._text import random_string


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
        created: EventHubSource | None = None
        try:
            created = cognite_client.hosted_extractors.sources.create(my_hub)
            assert isinstance(created, EventHubSource)
            update = EventHubSourceUpdate(external_id=my_hub.external_id).event_hub_name.set("myNewEventHub")
            updated = cognite_client.hosted_extractors.sources.update(update)
            assert updated.event_hub_name == "myNewEventHub"
            retrieved = cognite_client.hosted_extractors.sources.retrieve(created.external_id)
            assert retrieved is not None
            assert retrieved.external_id == created.external_id
            assert retrieved.event_hub_name == "myNewEventHub"

            cognite_client.hosted_extractors.sources.delete(created.external_id)

            with pytest.raises(CogniteAPIError):
                cognite_client.hosted_extractors.sources.retrieve(created.external_id)

            cognite_client.hosted_extractors.sources.retrieve(created.external_id, ignore_unknown_ids=True)

        finally:
            if created:
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
        created: EventHubSource | None = None
        try:
            created = cognite_client.hosted_extractors.sources.create(my_hub)

            my_new_hub = EventHubSourceWrite(
                external_id=created.external_id,
                host="updatedHost",
                key_name="updatedKeyName",
                key_value="updatedKey",
                event_hub_name="updatedEventHub",
            )

            updated = cognite_client.hosted_extractors.sources.update(my_new_hub)

            assert updated.host == my_new_hub.host
        finally:
            if created:
                cognite_client.hosted_extractors.sources.delete(created.external_id, ignore_unknown_ids=True)
