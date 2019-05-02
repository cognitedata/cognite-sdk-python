from cognite.client import CogniteClient

COGNITE_CLIENT = CogniteClient()


class TestRawDatabasesAPI:
    def test_list_databases(self):
        dbs = COGNITE_CLIENT.raw.databases.list()
        assert len(dbs) > 0


class TestRawTablesAPI:
    def test_list_tables(self):
        tables = COGNITE_CLIENT.raw.tables.list(db_name="test__database1")
        assert len(tables) == 3


class TestRawRowsAPI:
    def test_list_rows(self):
        rows = COGNITE_CLIENT.raw.rows.list(db_name="test__database1", table_name="test__table_1")
        assert 2000 == len(rows)

    def test_get_row(self):
        row = COGNITE_CLIENT.raw.rows.get(db_name="test__database1", table_name="test__table_1", key="1")
        assert {"c{}".format(i): "1_{}".format(i) for i in range(10)} == row.columns
