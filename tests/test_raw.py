import re

import numpy as np
import pandas as pd
import pytest

from cognite import raw
from cognite._utils import APIError
from cognite.data_objects import RawResponse

DB_NAME = 'test_db'
TABLE_NAME = 'test_table'
ROW_KEY = 'test_key'
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
            assert re.match("Database named {} not found".format(DB_NAME), str(e.value))

# TODO: Caching issues in Raw API causing a lot of 500s. Omitting these tests for the time being.
# class TestTables:
#     @pytest.fixture(autouse=True)
#     def create_database(self):
#         print('creatin table')
#         print(raw.create_databases([DB_NAME]).to_json())
#         yield
#         print('deletein table')
#         raw.delete_databases([DB_NAME])
#
#     @pytest.fixture(scope='class')
#     def tables(self):
#         yield raw.get_tables(database_name=DB_NAME)
#
#     def test_create_tables(self):
#         response = raw.create_tables(DB_NAME, [TABLE_NAME])
#         # assert isinstance(response, RawObject)
#         assert response.to_json()[0]['tableName'] == TABLE_NAME
#
#     def test_tables_response_length(self, tables):
#         print(tables.to_json())
#         assert len(tables.to_json()) > 0
#
#     def test_tables_object_output_formats(self, tables):
#         assert isinstance(tables, RawObject)
#         assert isinstance(tables.to_json(), list)
#         assert isinstance(tables.to_ndarray(), np.ndarray)
#         assert isinstance(tables.to_pandas(), pd.DataFrame)
#
#     def test_delete_tables(self):
#         response = raw.delete_tables(database_name=DB_NAME, table_names=[TABLE_NAME])
#         assert response == {}
#         with pytest.raises(APIError) as e:
#             raw.delete_tables(DB_NAME, [TABLE_NAME])
#             print(e.code)
#             assert 0

#
#
# @pytest.fixture(scope='module')
# def rows():
#     return raw.get_rows(database_name=DB_NAME, table_name=TABLE_NAME, limit=1)
#
#
# def test_create_rows():
#     response = raw.create_rows(DB_NAME, TABLE_NAME, rows=[RawRowDTO(key=ROW_KEY, columns=ROW_COLUMNS)])
#     assert response == {}
#
#
# # Test getting rows and their resulting output formats
# def test_databases_output_formats(databases):
#
#

#
#

#
#

#
#
# def test_rows_response_length(rows):
#
#
#
#
# def test_rows_object_output_formats(rows):
#     assert isinstance(rows, RawObject)
#     assert isinstance(rows.to_json(), list)
#     assert isinstance(rows.to_ndarray(), np.ndarray)
#     assert isinstance(rows.to_pandas(), pd.DataFrame)
