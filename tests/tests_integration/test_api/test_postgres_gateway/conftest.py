from __future__ import annotations

import time
from collections.abc import Iterator

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.postgres_gateway import SessionCredentials, User, UserWrite


@pytest.fixture(scope="session", autouse=True)
def purge_leftover_users(cognite_client: CogniteClient) -> None:
    cutoff = time.time() * 1000 - (10 * 60 * 1000)  # 10 minutes ago
    existing = cognite_client.postgres_gateway.users.list(limit=-1)
    stale = [u.username for u in existing if u.created_time < cutoff]
    cognite_client.postgres_gateway.users.delete(stale, ignore_unknown_ids=True)


@pytest.fixture
def fresh_credentials(cognite_client: CogniteClient) -> Iterator[SessionCredentials]:
    new_session = cognite_client.iam.sessions.create(session_type="ONESHOT_TOKEN_EXCHANGE")
    yield SessionCredentials(nonce=new_session.nonce)
    cognite_client.iam.sessions.revoke(new_session.id)


@pytest.fixture
def another_fresh_credentials(cognite_client: CogniteClient) -> Iterator[SessionCredentials]:
    new_session = cognite_client.iam.sessions.create(session_type="ONESHOT_TOKEN_EXCHANGE")
    yield SessionCredentials(nonce=new_session.nonce)
    cognite_client.iam.sessions.revoke(new_session.id)


@pytest.fixture
def one_user(cognite_client: CogniteClient, fresh_credentials: SessionCredentials) -> Iterator[User]:
    my_user = UserWrite(credentials=fresh_credentials)
    created = cognite_client.postgres_gateway.users.create(my_user)
    yield created
    cognite_client.postgres_gateway.users.delete(created.username, ignore_unknown_ids=True)
