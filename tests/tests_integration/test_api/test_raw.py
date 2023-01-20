import pytest

from cognite.client.data_classes import Row
from cognite.client.utils._auxiliary import random_string


@pytest.fixture(scope="session")
def new_database_with_table(cognite_client):
    db_name = "db_" + random_string(10)
    table_name = "table_" + random_string(10)
    db = cognite_client.raw.databases.create(db_name)
    table = cognite_client.raw.tables.create(db_name, table_name)
    yield db, table
    cognite_client.raw.databases.delete(name=db.name, recursive=True)


class TestRawDatabasesAPI:
    def test_list_databases(self, cognite_client):
        dbs = cognite_client.raw.databases.list()
        assert len(dbs) > 0

    def test_create_and_delete_database(self, new_database_with_table):
        pass


class TestRawTablesAPI:
    def test_list_tables(self, cognite_client):
        tables = cognite_client.raw.tables.list(db_name="test__database1")
        assert len(tables) == 3

    def test_create_and_delete_table(self, cognite_client, new_database_with_table):
        db, _ = new_database_with_table
        table_name = "table_" + random_string(10)
        table = cognite_client.raw.tables.create(db.name, table_name)
        assert table in cognite_client.raw.tables.list(db.name)
        cognite_client.raw.tables.delete(db.name, table.name)
        assert table not in cognite_client.raw.tables.list(db.name)


class TestRawRowsAPI:
    def test_list_rows(self, cognite_client):
        rows = cognite_client.raw.rows.list(db_name="test__database1", table_name="test__table_1", limit=10000)
        assert 2000 == len(rows)
        assert 10 == len(rows[0].columns.keys())

    def test_list_rows_w_parallel_cursors(self, cognite_client):
        randstr = random_string(32)
        num_rows = 30000
        rows_to_insert = [Row(key=str(i), columns={"a": 1}) for i in range(num_rows)]
        cognite_client.raw.rows.insert(randstr, randstr, row=rows_to_insert, ensure_parent=True)

        rows = cognite_client.raw.rows.list(db_name=randstr, table_name=randstr, limit=num_rows)
        rows_par = cognite_client.raw.rows.list(db_name=randstr, table_name=randstr, limit=-1)

        assert num_rows == len(rows) == len(rows_par)
        assert 1 == len(rows[0].columns.keys()) == len(rows_par[0].columns.keys())

        assert {row.key for row in rows} == {row.key for row in rows_par}

    def test_list_rows_cols(self, cognite_client):
        rows_list = cognite_client.raw.rows.list(
            db_name="test__database1", table_name="test__table_1", limit=10, columns=["c1", "c2"]
        )
        assert 10 == len(rows_list)
        for row in rows_list:
            assert {"c1", "c2"} == set(row.columns.keys())

    def test_iter_rows_cols(self, cognite_client):
        rows = cognite_client.raw.rows(
            db_name="test__database1", table_name="test__table_1", limit=10, columns=["c1", "c2"]
        )
        assert 10 == len([x for x in rows])
        for row in rows:
            assert {"c1", "c2"} == set(row.columns.keys())

    def test_retrieve_row(self, cognite_client):
        row = cognite_client.raw.rows.retrieve(db_name="test__database1", table_name="test__table_1", key="1")
        assert {f"c{i}": f"1_{i}" for i in range(10)} == row.columns

    def test_insert_and_delete_rows(self, cognite_client, new_database_with_table):
        db, table = new_database_with_table
        rows = {"r1": {"c1": "v1", "c2": "v1"}, "r2": {"c1": "v2", "c2": "v2"}}
        cognite_client.raw.rows.insert(db.name, table.name, rows)
        assert 2 == len(table.rows())
        cognite_client.raw.rows.delete(db.name, table.name, ["r1", "r2"])
        assert 0 == len(table.rows())

    @pytest.mark.dsl
    def test_insert_and_retrieve_dataframe(self, cognite_client, new_database_with_table):
        import pandas as pd

        db, table = new_database_with_table
        data = {"a": {"r1": 1, "r2": 1, "r3": 1}, "b": {"r1": None, "r2": None, "r3": None}}

        df = pd.DataFrame.from_dict(data)
        cognite_client.raw.rows.insert_dataframe(db.name, table.name, df)
        retrieved_df = cognite_client.raw.rows.retrieve_dataframe(db.name, table.name)

        pd.testing.assert_frame_equal(df.sort_index(), retrieved_df.sort_index())
        assert retrieved_df.to_dict() == data
