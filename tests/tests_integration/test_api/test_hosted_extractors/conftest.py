from __future__ import annotations

import time
from collections.abc import Iterator

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import DataSet, DataSetWrite
from cognite.client.data_classes.hosted_extractors import (
    Destination,
    DestinationWrite,
    EventHubSourceWrite,
    SessionWrite,
    Source,
)
from cognite.client.utils._text import random_string

HUB_SOURCE_PREFIX = "myNewHub-"
MQTT_SOURCE_PREFIX = "myMqttSource-"
UPDATE_SOURCE_PREFIX = "to-update-"
TEST_SOURCE_PREFIXES = (HUB_SOURCE_PREFIX, MQTT_SOURCE_PREFIX, UPDATE_SOURCE_PREFIX)


@pytest.fixture(scope="session", autouse=True)
def cleanup_stale_test_sources(cognite_client: CogniteClient) -> None:
    all_sources = cognite_client.hosted_extractors.sources.list(limit=None)
    now_ms = int(time.time() * 1000)
    # Clean up anyting older than 3 hours:
    stale = [
        s.external_id
        for s in all_sources
        if s.external_id
        and s.external_id.startswith(TEST_SOURCE_PREFIXES)
        and now_ms - s.created_time >= 3 * 60 * 60 * 1000  # type: ignore [attr-defined]
    ]
    if stale:
        cognite_client.hosted_extractors.sources.delete(stale, ignore_unknown_ids=True)


@pytest.fixture
def fresh_session(cognite_client: CogniteClient) -> Iterator[SessionWrite]:
    new_session = cognite_client.iam.sessions.create(session_type="ONESHOT_TOKEN_EXCHANGE")
    yield SessionWrite(nonce=new_session.nonce)
    cognite_client.iam.sessions.revoke(new_session.id)


@pytest.fixture(scope="session")
def a_data_set(cognite_client: CogniteClient) -> DataSet:
    ds = DataSetWrite(external_id="test-dataset-hosted-extractor", name="test-dataset-hosted-extractor")
    retrieved = cognite_client.data_sets.retrieve(external_id=ds.external_id)
    if retrieved:
        return retrieved
    created = cognite_client.data_sets.create(ds)
    return created


@pytest.fixture
def one_destination(cognite_client: CogniteClient, fresh_session: SessionWrite) -> Iterator[Destination]:
    my_dest = DestinationWrite(
        external_id=f"myNewDestination-{random_string(10)}",
        credentials=fresh_session,
    )
    created = cognite_client.hosted_extractors.destinations.create(my_dest)
    yield created

    cognite_client.hosted_extractors.destinations.delete(created.external_id, ignore_unknown_ids=True)


@pytest.fixture(scope="session")
def one_event_hub_source(cognite_client: CogniteClient, os_and_py_version: str) -> Source:
    my_hub = EventHubSourceWrite(
        external_id=f"{HUB_SOURCE_PREFIX}{os_and_py_version}",
        host="myHost",
        key_name="myKeyName",
        key_value="myKey",
        event_hub_name="myEventHub",
    )
    retrieved = cognite_client.hosted_extractors.sources.retrieve(my_hub.external_id, ignore_unknown_ids=True)
    if retrieved:
        return retrieved
    return cognite_client.hosted_extractors.sources.create(my_hub)
