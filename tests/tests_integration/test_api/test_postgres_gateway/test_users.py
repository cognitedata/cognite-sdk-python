import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.postgres_gateway import (
    SessionCredentials,
    User,
    UserList,
    UserUpdate,
    UserWrite,
)
from cognite.client.exceptions import CogniteAPIError


class TestUsers:
    def test_create_update_retrieve_delete(
        self,
        cognite_client: CogniteClient,
        fresh_credentials: SessionCredentials,
        another_fresh_credentials: SessionCredentials,
    ) -> None:
        my_user = UserWrite(credentials=fresh_credentials)
        created: User | None = None
        try:
            created = cognite_client.postgres_gateway.users.create(my_user)
            assert isinstance(created, User)
            update = UserUpdate(created.username).credentials.set(another_fresh_credentials)
            updated = cognite_client.postgres_gateway.users.update(update)
            assert updated.username == created.username
            retrieved = cognite_client.postgres_gateway.users.retrieve(created.username)
            assert retrieved is not None
            assert retrieved.username == created.username

            cognite_client.postgres_gateway.users.delete(created.username)

            with pytest.raises(CogniteAPIError):
                cognite_client.postgres_gateway.users.retrieve(created.username)

            cognite_client.postgres_gateway.users.retrieve(created.username, ignore_unknown_ids=True)

        finally:
            if created:
                cognite_client.postgres_gateway.users.delete(created.username, ignore_unknown_ids=True)

    @pytest.mark.usefixtures("one_user")
    def test_list(self, cognite_client: CogniteClient) -> None:
        res = cognite_client.postgres_gateway.users.list(limit=1)
        assert len(res) == 1
        assert isinstance(res, UserList)
