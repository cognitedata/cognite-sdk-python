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
    ViewTableOptions,
    ViewTableWrite,
)
from cognite.client.exceptions import CogniteAPIError


@pytest.fixture
def one_space(cognite_client: CogniteClient) -> Space:
    my_space = SpaceApply(
        space="my_space",
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


class TestTables:
    def test_create_retrieve_delete(self, cognite_client: CogniteClient, one_user: User, one_view: View) -> None:
        my_table = ViewTableWrite(
            tablename="my_table",
            options=ViewTableOptions(space=one_view.space, external_id=one_view.external_id, version=one_view.version),
        )
        username = one_user.username
        tablename = my_table.tablename
        created: Table | None = None
        try:
            created = cognite_client.postgres_gateway.tables.create(my_table, username)
            assert isinstance(created, Table | UnknownCogniteObject)

            retrieved = cognite_client.postgres_gateway.tables.retrieve(tablename, username)
            assert retrieved is not None

            cognite_client.postgres_gateway.tables.delete(tablename, username)

            with pytest.raises(CogniteAPIError):
                cognite_client.postgres_gateway.tables.retrieve(tablename, username)

            cognite_client.postgres_gateway.tables.retrieve(tablename, username, ignore_unknown_ids=True)

        finally:
            if created:
                cognite_client.postgres_gateway.tables.delete(tablename, username, ignore_unknown_ids=True)

    @pytest.mark.usefixtures("one_table")
    def test_list(self, cognite_client: CogniteClient, one_user: User) -> None:
        res = cognite_client.postgres_gateway.tables.list(username=one_user.username, limit=1)
        assert len(res) == 1
        assert isinstance(res, TableList)
