from __future__ import annotations

import platform

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


@pytest.fixture
def fresh_session(cognite_client: CogniteClient) -> SessionWrite:
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
def one_destination(cognite_client: CogniteClient, fresh_session: SessionWrite) -> Destination:
    my_dest = DestinationWrite(
        external_id=f"myNewDestination-{random_string(10)}",
        credentials=fresh_session,
    )
    created = cognite_client.hosted_extractors.destinations.create(my_dest)
    yield created

    cognite_client.hosted_extractors.destinations.delete(created.external_id, ignore_unknown_ids=True)


@pytest.fixture(scope="session")
def one_event_hub_source(cognite_client: CogniteClient) -> Source:
    my_hub = EventHubSourceWrite(
        external_id=f"myNewHub-{platform.system()}-{platform.python_version()}",
        host="myHost",
        key_name="myKeyName",
        key_value="myKey",
        event_hub_name="myEventHub",
    )
    retrieved = cognite_client.hosted_extractors.sources.retrieve(my_hub.external_id, ignore_unknown_ids=True)
    if retrieved:
        return retrieved
    return cognite_client.hosted_extractors.sources.create(my_hub)
