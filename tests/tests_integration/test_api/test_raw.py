import time

import pytest

from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils import _utils

COGNITE_CLIENT = CogniteClient()


@pytest.fixture(scope="session")
def new_database_with_table():
    db_name = "db_" + _utils.random_string(10)
    table_name = "table_" + _utils.random_string(10)
    db = COGNITE_CLIENT.raw.databases.create(db_name)
    table = COGNITE_CLIENT.raw.tables.create(db_name, table_name)
    yield db, table
    COGNITE_CLIENT.raw.databases.delete(db.name)


class TestRawDatabasesAPI:
    def test_list_databases(self):
        dbs = COGNITE_CLIENT.raw.databases.list()
        assert len(dbs) > 0

    def test_create_and_delete_database(self, new_database_with_table):
        pass


class TestRawTablesAPI:
    def test_list_tables(self):
        tables = COGNITE_CLIENT.raw.tables.list(db_name="test__database1")
        assert len(tables) == 3

    def test_create_and_delete_table(self, new_database_with_table):
        db, _ = new_database_with_table
        table_name = "table_" + _utils.random_string(10)
        table = COGNITE_CLIENT.raw.tables.create(db.name, table_name)
        assert table in COGNITE_CLIENT.raw.tables.list(db.name)
        COGNITE_CLIENT.raw.tables.delete(db.name, table.name)
        assert not table in COGNITE_CLIENT.raw.tables.list(db.name)


class TestRawRowsAPI:
    def test_list_rows(self):
        rows = COGNITE_CLIENT.raw.rows.list(db_name="test__database1", table_name="test__table_1")
        assert 2000 == len(rows)

    def test_retrieve_row(self):
        row = COGNITE_CLIENT.raw.rows.retrieve(db_name="test__database1", table_name="test__table_1", key="1")
        assert {"c{}".format(i): "1_{}".format(i) for i in range(10)} == row.columns

    def test_insert_and_delete_rows(self, new_database_with_table):
        db, table = new_database_with_table
        rows = {"r1": {"c1": "v1", "c2": "v1"}, "r2": {"c1": "v2", "c2": "v2"}}
        COGNITE_CLIENT.raw.rows.insert(db.name, table.name, rows)
        assert 2 == len(table.rows())
        COGNITE_CLIENT.raw.rows.delete(db.name, table.name, ["r1", "r2"])
        assert 0 == len(table.rows())
