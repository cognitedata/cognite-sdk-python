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

DESTINATION_PREFIX = "myNewDestination-"
DESTINATION_FOR_TESTING_PREFIX = "myNewDestinationForTesting-"
UPDATE_DESTINATION_PREFIX = "toupdate-"
TEST_DESTINATION_PREFIXES = (DESTINATION_PREFIX, DESTINATION_FOR_TESTING_PREFIX, UPDATE_DESTINATION_PREFIX)

# Sources and Destinatinos are repeat offenders for hitting max limits because they are
# quite low. We add specific cleanup for anything older than...:
DELETE_THRESHOLD = 1 * 60 * 60 * 1000  # 1 hour in ms
NOW_MS = int(time.time() * 1000)


@pytest.fixture(scope="session", autouse=True)
def cleanup_stale_test_destinations(cognite_client: CogniteClient) -> None:
    # max is low number like e.g. 50 per project, safe to list all:
    all_destinations = cognite_client.hosted_extractors.destinations.list(limit=None)
    stale = [
        d.external_id
        for d in all_destinations
        if d.external_id
        and d.external_id.startswith(TEST_DESTINATION_PREFIXES)
        and NOW_MS - d.created_time >= DELETE_THRESHOLD  # type: ignore [operator]
    ]
    if stale:
        cognite_client.hosted_extractors.destinations.delete(stale, force=True, ignore_unknown_ids=True)


@pytest.fixture(scope="session", autouse=True)
def cleanup_stale_test_sources(cognite_client: CogniteClient) -> None:
    all_sources = cognite_client.hosted_extractors.sources.list(limit=None)
    stale = [
        s.external_id
        for s in all_sources
        if s.external_id
        and s.external_id.startswith(TEST_SOURCE_PREFIXES)
        and NOW_MS - s.created_time >= DELETE_THRESHOLD  # type: ignore [attr-defined]
    ]
    if stale:
        cognite_client.hosted_extractors.sources.delete(stale, force=True, ignore_unknown_ids=True)


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
        external_id=f"{DESTINATION_PREFIX}{random_string(10)}",
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
