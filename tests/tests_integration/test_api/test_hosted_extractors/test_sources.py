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
def one_event_hub_source(cognite_client: CogniteClient) -> SourceList:
    my_hub = EventHubSourceWrite(
        external_id=f"myNewHub-{random_string(10)}",
        host="myHost",
        key_name="myKeyName",
        key_value="myKey",
        event_hub_name="myEventHub",
    )
    retrieved = cognite_client.hosted_extractors.sources.retrieve(my_hub.external_id, ignore_unknown_ids=True)
    if retrieved:
        return retrieved
    created = cognite_client.hosted_extractors.sources.create(my_hub)
    return SourceList([created])


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
