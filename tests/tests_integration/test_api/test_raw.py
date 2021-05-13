import pytest

from cognite.client import CogniteClient, utils

COGNITE_CLIENT = CogniteClient()


@pytest.fixture(scope="session")
def new_database_with_table():
    db_name = "db_" + utils._auxiliary.random_string(10)
    table_name = "table_" + utils._auxiliary.random_string(10)
    db = COGNITE_CLIENT.raw.databases.create(db_name)
    table = COGNITE_CLIENT.raw.tables.create(db_name, table_name)
    yield db, table
    COGNITE_CLIENT.raw.databases.delete(name=db.name, recursive=True)


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
        table_name = "table_" + utils._auxiliary.random_string(10)
        table = COGNITE_CLIENT.raw.tables.create(db.name, table_name)
        assert table in COGNITE_CLIENT.raw.tables.list(db.name)
        COGNITE_CLIENT.raw.tables.delete(db.name, table.name)
        assert table not in COGNITE_CLIENT.raw.tables.list(db.name)


class TestRawRowsAPI:
    def test_list_rows(self):
        rows = COGNITE_CLIENT.raw.rows.list(db_name="test__database1", table_name="test__table_1", limit=-1)
        assert 2000 == len(rows)
        assert 10 == len(rows[0].columns.keys())

    def test_list_rows_cols(self):
        rows = COGNITE_CLIENT.raw.rows(
            db_name="test__database1", table_name="test__table_1", limit=10, columns=["c1", "c2"]
        )
        rows_list = COGNITE_CLIENT.raw.rows.list(
            db_name="test__database1", table_name="test__table_1", limit=10, columns=["c1", "c2"]
        )
        assert 10 == len([x for x in rows])
        assert 10 == len(rows_list)
        for row in rows:
            assert {"c1", "c2"} == set(row.columns.keys())
        for row in rows_list:
            assert {"c1", "c2"} == set(row.columns.keys())

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
