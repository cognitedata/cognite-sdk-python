import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.postgres_gateway import (
    Column,
    RawTableOptions,
    RawTableWrite,
    Table,
    TableList,
    User,
    ViewTableOptions,
    ViewTableWrite,
)
from cognite.client.exceptions import CogniteAPIError


@pytest.fixture
def one_table(cognite_client: CogniteClient, one_user: User) -> Table:
    my_table = RawTableWrite(
        tablename="my_table",
        options=RawTableOptions(
            database="my_database",
            table="my_table",
            primary_key="id",
        ),
        columns=[
            Column(name="id", type="BIGINT"),
            Column(name="name", type="TEXT"),
        ],
    )
    created = cognite_client.postgres_gateway.tables.create(my_table, one_user.username)
    yield created
    cognite_client.postgres_gateway.tables.delete(created.tablename, one_user.username, ignore_unknown_ids=True)


class TestUsers:
    def test_create_retrieve_delete(self, cognite_client: CogniteClient, one_user: User) -> None:
        my_table = ViewTableWrite(
            tablename="my_table", options=ViewTableOptions(space="my_space", external_id="my_view", version="v1")
        )
        username = one_user.username
        created: Table | None = None
        try:
            created = cognite_client.postgres_gateway.tables.create(my_table, username)
            assert isinstance(created, Table)

            retrieved = cognite_client.postgres_gateway.users.retrieve(created.tablename, username)
            assert retrieved is not None

            cognite_client.postgres_gateway.tables.delete(created.tablename, username)

            with pytest.raises(CogniteAPIError):
                cognite_client.postgres_gateway.tables.retrieve(created.tablename, username)

            cognite_client.postgres_gateway.tables.retrieve(created.tablename, username, ignore_unknown_ids=True)

        finally:
            if created:
                cognite_client.postgres_gateway.tables.delete(created.tablename, username, ignore_unknown_ids=True)

    @pytest.mark.usefixtures("one_table")
    def test_list(self, cognite_client: CogniteClient, one_user: User) -> None:
        res = cognite_client.postgres_gateway.tables.list(username=one_user.username, limit=1)
        assert len(res) == 1
        assert isinstance(res, TableList)
