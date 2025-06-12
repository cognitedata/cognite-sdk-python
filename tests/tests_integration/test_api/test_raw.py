import random

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import Row
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._text import random_string


@pytest.fixture(scope="session")
def new_database_with_table(cognite_client):
    db_name = "db_" + random_string(10)
    table_name = "table_" + random_string(10)
    db = cognite_client.raw.databases.create(db_name)
    table = cognite_client.raw.tables.create(db_name, table_name)
    yield db, table
    cognite_client.raw.databases.delete(name=db.name, recursive=True)


@pytest.fixture(scope="session")
def db_database(cognite_client: CogniteClient) -> str:
    db_name = "test__database1"
    all_dbs = cognite_client.raw.databases.list(limit=-1)
    if db_name not in all_dbs.as_names():
        db = cognite_client.raw.databases.create(db_name)
        for i in range(1, 4):
            table_name = f"test__table_{i}"
            table = cognite_client.raw.tables.create(db.name, table_name)
            rows = [Row(key=str(j), columns={f"c{k}": f"{j}_{k}" for k in range(10)}) for j in range(2000)]
            cognite_client.raw.rows.insert(db.name, table.name, rows)
    return db_name


class TestRawDatabasesAPI:
    def test_list_databases(self, cognite_client):
        dbs = cognite_client.raw.databases.list()
        assert len(dbs) > 0

    def test_create_and_delete_database(self, new_database_with_table):
        assert True


class TestRawTablesAPI:
    def test_list_tables(self, cognite_client, db_database: str):
        tables = cognite_client.raw.tables.list(db_name=db_database)
        assert len(tables) == 3

    def test_create_and_delete_table(self, cognite_client, new_database_with_table):
        db, _ = new_database_with_table
        table_name = "table_" + random_string(10)
        table = cognite_client.raw.tables.create(db.name, table_name)
        assert table in cognite_client.raw.tables.list(db.name)
        cognite_client.raw.tables.delete(db.name, table.name)
        assert table not in cognite_client.raw.tables.list(db.name)

    def test_create_existing(self, cognite_client, new_database_with_table):
        db, table = new_database_with_table
        with pytest.raises(CogniteAPIError, match="already created"):
            cognite_client.raw.tables.create(db.name, table.name)

    def test_delete_missing(self, cognite_client: CogniteClient, new_database_with_table):
        db, _ = new_database_with_table
        with pytest.raises(CogniteAPIError, match="not found"):
            cognite_client.raw.tables.delete(db.name, "i-dont-exist")


class TestRawRowsAPI:
    def test_list_rows(self, cognite_client, db_database: str):
        rows = cognite_client.raw.rows.list(db_name=db_database, table_name="test__table_1", limit=10000)
        assert 2000 == len(rows)
        assert 10 == len(rows[0].columns.keys())

    def test_rows_with_parallel_cursors(self, cognite_client):
        randstr = random_string(32)
        num_rows = random.randint(15000, 30000)
        rows_to_insert = [Row(key=str(i), columns={"a": 1}) for i in range(num_rows)]
        try:
            cognite_client.raw.rows.insert(randstr, randstr, row=rows_to_insert, ensure_parent=True)

            rows = cognite_client.raw.rows.list(randstr, randstr, limit=num_rows)
            rows_par = cognite_client.raw.rows.list(randstr, randstr, limit=None, partitions=2)
            rows_iter = list(cognite_client.raw.rows(randstr, randstr, limit=None, partitions=3, chunk_size=5000))

            assert num_rows == len(rows) == len(rows_par)
            assert 1 == len(rows[0].columns) == len(rows_par[0].columns) == len(rows_iter[0][0].columns)

            keys = {row.key for row in rows}
            keys_par = {row.key for row in rows_par}
            keys_iter = {row.key for row_list in rows_iter for row in row_list}
            assert keys == keys_par == keys_iter
        finally:
            cognite_client.raw.databases.delete(randstr, recursive=True)

    def test_list_rows_cols(self, cognite_client, db_database: str) -> None:
        rows_list = cognite_client.raw.rows.list(
            db_name=db_database, table_name="test__table_1", limit=10, columns=["c1", "c2"]
        )
        assert 10 == len(rows_list)
        for row in rows_list:
            assert {"c1", "c2"} == set(row.columns.keys())

    def test_iter_rows_cols(self, cognite_client, db_database: str) -> None:
        rows = cognite_client.raw.rows(db_name=db_database, table_name="test__table_1", limit=10, columns=["c1", "c2"])
        assert 10 == len([x for x in rows])
        for row in rows:
            assert {"c1", "c2"} == set(row.columns.keys())

    def test_retrieve_row(self, cognite_client, db_database: str) -> None:
        row = cognite_client.raw.rows.retrieve(db_name=db_database, table_name="test__table_1", key="1")
        assert {f"c{i}": f"1_{i}" for i in range(10)} == row.columns

    def test_insert_and_delete_rows(self, cognite_client, new_database_with_table):
        db, table = new_database_with_table
        rows = {"r1": {"c1": "v1", "c2": "v1"}, "r2": {"c1": "v2", "c2": "v2"}}
        cognite_client.raw.rows.insert(db.name, table.name, rows)
        assert 2 == len(table.rows())
        cognite_client.raw.rows.delete(db.name, table.name, ["r1", "r2"])
        assert 0 == len(table.rows())

    def test_delete_missing_key(self, cognite_client, new_database_with_table):
        db, table = new_database_with_table
        # endpoint is idempotent so this should not raise
        cognite_client.raw.rows.delete(db.name, table.name, ["i-dont-exist"])

    def test_insert_existing_key(self, cognite_client, new_database_with_table):
        db, table = new_database_with_table
        rows = {"r1": {"c1": "v1", "c2": "v1"}, "r2": {"c1": "v2", "c2": "v2"}}
        cognite_client.raw.rows.insert(db.name, table.name, rows)
        # endpoint is idempotent so this should not raise
        cognite_client.raw.rows.insert(db.name, table.name, rows)

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

    @pytest.mark.dsl
    def test_insert_dataframe__index_has_duplicates(self, cognite_client):
        import pandas as pd

        df = pd.DataFrame({"aa": range(4), "bb": "value"}, index=list("abca"))

        with pytest.raises(ValueError, match="^Dataframe index is not unique"):
            cognite_client.raw.rows.insert_dataframe("db", "table", df)
