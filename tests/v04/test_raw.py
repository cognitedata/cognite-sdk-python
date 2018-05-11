from random import randint

import numpy as np
import pandas as pd
import pytest

from cognite._utils import APIError
from cognite.v04 import raw
from cognite.v04.dto import RawResponse, RawRow

DB_NAME = None
TABLE_NAME = None
ROW_KEY = None
ROW_COLUMNS = None


@pytest.fixture(autouse=True, scope='class')
def db_name():
    global DB_NAME
    DB_NAME = 'test_db_{}'.format(randint(1, 2 ** 53 - 1))


@pytest.fixture(autouse=True, scope='class')
def table_name():
    global TABLE_NAME
    TABLE_NAME = 'test_table_{}'.format(randint(1, 2 ** 53 - 1))


@pytest.fixture(autouse=True, scope='class')
def row_key():
    global ROW_KEY
    ROW_KEY = 'test_key_{}'.format(randint(1, 2 ** 53 - 1))


@pytest.fixture(autouse=True, scope='class')
def row_columns():
    global ROW_COLUMNS
    ROW_COLUMNS = {'col1': 'val1'}


class TestDatabases:
    @pytest.fixture(scope='class')
    def databases(self):
        yield raw.get_databases()

    def test_create_databases(self):
        response = raw.create_databases([DB_NAME])
        assert isinstance(response, RawResponse)
        assert response.to_json()[0]['dbName'] == DB_NAME

    def test_databases_response_length(self, databases):
        assert len(databases.to_json()) > 0

    def test_get_databases_output_formats(self, databases):
        assert isinstance(databases, RawResponse)
        assert isinstance(databases.to_json(), list)
        assert isinstance(databases.to_ndarray(), np.ndarray)
        assert isinstance(databases.to_pandas(), pd.DataFrame)

    def test_delete_databases(self):
        response = raw.delete_databases([DB_NAME], recursive=True)
        assert response == {}
        with pytest.raises(APIError) as e:
            raw.delete_databases([DB_NAME])


class TestTables:
    @pytest.fixture(autouse=True, scope='class')
    def create_database(self):
        raw.create_databases([DB_NAME])
        yield
        raw.delete_databases([DB_NAME], recursive=True)

    @pytest.fixture(scope='class')
    def tables(self):
        yield raw.get_tables(DB_NAME)

    def test_create_tables(self):
        response = raw.create_tables(DB_NAME, [TABLE_NAME])
        # assert isinstance(response, RawObject)
        assert response.to_json()[0]['tableName'] == TABLE_NAME

    def test_tables_response_length(self, tables):
        assert len(tables.to_json()) > 0

    def test_tables_object_output_formats(self, tables):
        assert isinstance(tables, RawResponse)
        assert isinstance(tables.to_json(), list)
        assert isinstance(tables.to_ndarray(), np.ndarray)
        assert isinstance(tables.to_pandas(), pd.DataFrame)

    def test_delete_tables(self):
        response = raw.delete_tables(database_name=DB_NAME, table_names=[TABLE_NAME])
        assert response == {}
        with pytest.raises(APIError) as e:
            raw.delete_tables(DB_NAME, [TABLE_NAME])
            # assert re.match("{'code': 404, 'message': 'Did not find any dbs with the given names'}", str(e.value))
            # assert re.match("{'code': 404, 'message': 'No tables named test_table'}")


class TestRows:
    @pytest.fixture(autouse=True, scope='class')
    def create_database(self):
        raw.create_databases([DB_NAME])
        raw.create_tables(DB_NAME, [TABLE_NAME])
        yield
        raw.delete_databases([DB_NAME], recursive=True)

    def test_create_rows(self):
        response = raw.create_rows(DB_NAME, TABLE_NAME, rows=[RawRow(key=ROW_KEY, columns=ROW_COLUMNS)])
        assert response == {}

    def test_rows_response_length(self):
        rows = raw.get_rows(database_name=DB_NAME, table_name=TABLE_NAME).to_json()
        assert len(rows) == 1

    def test_rows_object_output_formats(self):
        row = raw.get_row(DB_NAME, TABLE_NAME, ROW_KEY)
        assert isinstance(row, RawResponse)
        assert isinstance(row.to_json(), list)
        assert isinstance(row.to_ndarray(), np.ndarray)
        assert isinstance(row.to_pandas(), pd.DataFrame)

    def test_delete_rows(self):
        response = raw.delete_rows(DB_NAME, TABLE_NAME, [RawRow(key=ROW_KEY, columns=ROW_COLUMNS)])
        assert response == {}
