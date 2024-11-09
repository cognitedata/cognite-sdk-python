from contextlib import suppress

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes._base import UnknownCogniteObject
from cognite.client.data_classes.data_modeling import (
    Container,
    ContainerApply,
    ContainerProperty,
    MappedPropertyApply,
    Space,
    SpaceApply,
    Text,
    View,
    ViewApply,
)
from cognite.client.data_classes.postgres_gateway import (
    Column,
    RawTableOptions,
    RawTableWrite,
    Table,
    TableList,
    User,
    ViewTableWrite,
)
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._text import random_string


@pytest.fixture
def one_space(cognite_client: CogniteClient) -> Space:
    my_space = SpaceApply(
        space=f"my_space-{random_string(10)}",
    )
    created = cognite_client.data_modeling.spaces.apply(my_space)
    yield created
    cognite_client.data_modeling.spaces.delete(created.space)


@pytest.fixture
def one_container(cognite_client: CogniteClient, one_space: Space) -> Container:
    my_container = ContainerApply(
        space=one_space.space, external_id="my_container", properties={"name": ContainerProperty(type=Text())}
    )
    created = cognite_client.data_modeling.containers.apply(my_container)
    yield created
    cognite_client.data_modeling.containers.delete(created.as_id())


@pytest.fixture
def one_view(cognite_client: CogniteClient, one_space: Space, one_container: Container) -> View:
    my_view = ViewApply(
        space=one_space.space,
        external_id="my_view",
        version="v1",
        properties={"name": MappedPropertyApply(one_container.as_id(), "name")},
    )
    created = cognite_client.data_modeling.views.apply(my_view)
    yield created
    cognite_client.data_modeling.views.delete(created.as_id())


@pytest.fixture
def one_raw_table(cognite_client: CogniteClient) -> tuple[str, str]:
    db_table_pair = f"my_database-{random_string(10)}", "my_table_postgres_gateway"

    cognite_client.raw.databases.create(db_table_pair[0])
    cognite_client.raw.tables.create(*db_table_pair)

    yield db_table_pair
    with suppress(CogniteAPIError):
        cognite_client.raw.tables.delete(*db_table_pair)
    with suppress(CogniteAPIError):
        cognite_client.raw.databases.delete(db_table_pair[0])


@pytest.fixture
def one_table(cognite_client: CogniteClient, one_user: User, one_raw_table: tuple[str, str]) -> Table:
    my_table = RawTableWrite(
        tablename=f"my_table-{random_string(10)}",
        options=RawTableOptions(
            database=one_raw_table[0],
            table=one_raw_table[1],
        ),
        columns=[
            Column(name="id", type="BIGINT"),
            Column(name="name", type="TEXT"),
        ],
    )
    created = cognite_client.postgres_gateway.tables.create(one_user.username, my_table)
    yield created
    cognite_client.postgres_gateway.tables.delete(one_user.username, created.tablename, ignore_unknown_ids=True)


class TestTables:
    def test_create_retrieve_delete(self, cognite_client: CogniteClient, one_user: User, one_view: View) -> None:
        my_table = ViewTableWrite(
            tablename="my_table",
            options=one_view.as_id(),
        )
        username = one_user.username
        tablename = my_table.tablename
        created: Table | None = None
        try:
            created = cognite_client.postgres_gateway.tables.create(username, my_table)
            assert isinstance(created, Table | UnknownCogniteObject)

            retrieved = cognite_client.postgres_gateway.tables.retrieve(username, tablename)
            assert retrieved is not None

            cognite_client.postgres_gateway.tables.delete(username, tablename)

            with pytest.raises(CogniteAPIError):
                cognite_client.postgres_gateway.tables.retrieve(username, tablename)

            result = cognite_client.postgres_gateway.tables.retrieve(username, tablename, ignore_unknown_ids=True)

            assert result is None
        finally:
            if created:
                cognite_client.postgres_gateway.tables.delete(username, tablename, ignore_unknown_ids=True)

    @pytest.mark.usefixtures("one_table")
    def test_list(self, cognite_client: CogniteClient, one_user: User) -> None:
        res = cognite_client.postgres_gateway.tables.list(username=one_user.username, limit=1)
        assert len(res) == 1
        assert isinstance(res, TableList)
