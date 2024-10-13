from __future__ import annotations

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.postgres_gateway import SessionCredentials, User, UserWrite


@pytest.fixture
def fresh_credentials(cognite_client: CogniteClient) -> SessionCredentials:
    new_session = cognite_client.iam.sessions.create(session_type="ONESHOT_TOKEN_EXCHANGE")
    yield SessionCredentials(nonce=new_session.nonce)
    cognite_client.iam.sessions.revoke(new_session.id)


@pytest.fixture
def another_fresh_credentials(cognite_client: CogniteClient) -> SessionCredentials:
    new_session = cognite_client.iam.sessions.create(session_type="ONESHOT_TOKEN_EXCHANGE")
    yield SessionCredentials(nonce=new_session.nonce)
    cognite_client.iam.sessions.revoke(new_session.id)


@pytest.fixture
def one_user(cognite_client: CogniteClient, fresh_credentials: SessionCredentials) -> User:
    my_user = UserWrite(credentials=fresh_credentials)
    created = cognite_client.postgres_gateway.users.create(my_user)
    yield created
    cognite_client.postgres_gateway.users.delete(created.username, ignore_unknown_ids=True)
