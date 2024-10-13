import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.postgres_gateway import (
    Table,
    TableList,
    TableWrite,
    User,
)
from cognite.client.exceptions import CogniteAPIError


class TestUsers:
    def test_create_retrieve_delete(self, cognite_client: CogniteClient, one_user: User) -> None:
        my_table = TableWrite()
        created: Table | None = None
        try:
            created = cognite_client.postgres_gateway.tables.create(my_table)
            assert isinstance(created, User)

            retrieved = cognite_client.postgres_gateway.users.retrieve(created.tablename, one_user.username)
            assert retrieved is not None

            cognite_client.postgres_gateway.tables.delete(created.username)

            with pytest.raises(CogniteAPIError):
                cognite_client.postgres_gateway.tables.retrieve(created.username)

            cognite_client.postgres_gateway.tables.retrieve(created.username, ignore_unknown_ids=True)

        finally:
            if created:
                cognite_client.postgres_gateway.tables.delete(created.username, ignore_unknown_ids=True)

    @pytest.mark.usefixtures("one_table")
    def test_list(self, cognite_client: CogniteClient, one_user: User) -> None:
        res = cognite_client.postgres_gateway.tables.list(username=one_user.username, limit=1)
        assert len(res) == 1
        assert isinstance(res, TableList)
