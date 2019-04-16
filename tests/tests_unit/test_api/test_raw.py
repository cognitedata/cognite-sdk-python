import re

import pytest

from cognite.client import CogniteClient, global_client
from cognite.client.api.raw import Database, DatabaseList, Row, RowList, Table, TableList
from tests.utils import jsgz_load

COGNITE_CLIENT = CogniteClient()
global_client.set(None)
RAW_API = COGNITE_CLIENT.raw


@pytest.fixture
def mock_raw_db_response(rsps):
    response_body = {"data": {"items": [{"name": "db1"}]}}

    url_pattern = re.compile(re.escape(RAW_API._base_url) + "/raw/dbs(?:/delete|$|\?.+)")
    rsps.assert_all_requests_are_fired = False

    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
    yield rsps


class TestRawDatabases:
    def test_create_single(self, mock_raw_db_response):
        res = RAW_API.databases.create(name="db1")
        assert isinstance(res, Database)
        assert COGNITE_CLIENT == res._client
        assert mock_raw_db_response.calls[0].response.json()["data"]["items"][0] == res.dump(camel_case=True)
        assert [{"name": "db1"}] == jsgz_load(mock_raw_db_response.calls[0].request.body)["items"]

    def test_create_multiple(self, mock_raw_db_response):
        res_list = RAW_API.databases.create(name=["db1"])
        assert isinstance(res_list, DatabaseList)
        for res in res_list:
            assert COGNITE_CLIENT == res._client
        assert COGNITE_CLIENT == res_list._client
        assert [{"name": "db1"}] == jsgz_load(mock_raw_db_response.calls[0].request.body)["items"]
        assert mock_raw_db_response.calls[0].response.json()["data"]["items"] == res_list.dump(camel_case=True)

    def test_list(self, mock_raw_db_response):
        res_list = RAW_API.databases.list()
        assert DatabaseList([Database("db1")]) == res_list

    def test_iter_single(self, mock_raw_db_response):
        for db in RAW_API.databases:
            assert mock_raw_db_response.calls[0].response.json()["data"]["items"][0] == db.dump(camel_case=True)

    def test_iter_chunk(self, mock_raw_db_response):
        for db in RAW_API.databases(chunk_size=1):
            assert mock_raw_db_response.calls[0].response.json()["data"]["items"] == db.dump(camel_case=True)

    def test_delete(self, mock_raw_db_response):
        res = RAW_API.databases.delete(name="db1")
        assert res is None
        assert [{"name": "db1"}] == jsgz_load(mock_raw_db_response.calls[0].request.body)["items"]

    def test_delete_multiple(self, mock_raw_db_response):
        res = RAW_API.databases.delete(name=["db1"])
        assert res is None
        assert [{"name": "db1"}] == jsgz_load(mock_raw_db_response.calls[0].request.body)["items"]


@pytest.fixture
def mock_raw_table_response(rsps):
    response_body = {"data": {"items": [{"name": "table1"}]}}

    url_pattern = re.compile(re.escape(RAW_API._base_url) + "/raw/dbs/db1/tables(?:/delete|$|\?.+)")
    rsps.assert_all_requests_are_fired = False

    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
    yield rsps


class TestRawTables:
    def test_create_single(self, mock_raw_table_response):
        res = RAW_API.tables.create("db1", name="table1")
        assert isinstance(res, Table)
        assert COGNITE_CLIENT == res._client
        assert mock_raw_table_response.calls[0].response.json()["data"]["items"][0] == res.dump(camel_case=True)
        assert [{"name": "table1"}] == jsgz_load(mock_raw_table_response.calls[0].request.body)["items"]

    def test_create_multiple(self, mock_raw_table_response):
        res_list = RAW_API.tables.create("db1", name=["table1"])
        assert isinstance(res_list, TableList)
        for res in res_list:
            assert COGNITE_CLIENT == res._client
        assert COGNITE_CLIENT == res_list._client
        assert [{"name": "table1"}] == jsgz_load(mock_raw_table_response.calls[0].request.body)["items"]
        assert mock_raw_table_response.calls[0].response.json()["data"]["items"] == res_list.dump(camel_case=True)

    def test_list(self, mock_raw_table_response):
        res_list = RAW_API.tables.list(db_name="db1")
        assert TableList([Table("table1")]) == res_list

    def test_iter_single(self, mock_raw_table_response):
        for db in RAW_API.tables(db_name="db1"):
            assert mock_raw_table_response.calls[0].response.json()["data"]["items"][0] == db.dump(camel_case=True)

    def test_iter_chunk(self, mock_raw_table_response):
        for db in RAW_API.tables("db1", chunk_size=1):
            assert mock_raw_table_response.calls[0].response.json()["data"]["items"] == db.dump(camel_case=True)

    def test_delete(self, mock_raw_table_response):
        res = RAW_API.tables.delete("db1", name="table1")
        assert res is None
        assert [{"name": "table1"}] == jsgz_load(mock_raw_table_response.calls[0].request.body)["items"]

    def test_delete_multiple(self, mock_raw_table_response):
        res = RAW_API.tables.delete(db_name="db1", name=["table1"])
        assert res is None
        assert [{"name": "table1"}] == jsgz_load(mock_raw_table_response.calls[0].request.body)["items"]


@pytest.fixture
def mock_raw_row_response(rsps):
    response_body = {"data": {"items": [{"key": "row1", "columns": {"c1": 1, "c2": "2"}}]}}

    url_pattern = re.compile(re.escape(RAW_API._base_url) + "/raw/dbs/db1/tables/table1/rows(?:/delete|/row1|$|\?.+)")
    rsps.assert_all_requests_are_fired = False

    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
    yield rsps


class TestRawRows:
    def test_get(self, mock_raw_row_response):
        res = RAW_API.rows.get(db_name="db1", table_name="table1", key="row1")
        assert mock_raw_row_response.calls[0].response.json()["data"]["items"][0] == res.dump(camel_case=True)
        assert mock_raw_row_response.calls[0].request.url.endswith("/rows/row1")

    def test_insert_w_rows_as_dict(self, mock_raw_row_response):
        res = RAW_API.rows.insert(
            db_name="db1", table_name="table1", row={"row1": {"c1": 1, "c2": "2"}}, ensure_parent=True
        )
        assert isinstance(res, Row)
        assert COGNITE_CLIENT == res._client
        assert mock_raw_row_response.calls[0].response.json()["data"]["items"][0] == res.dump(camel_case=True)
        assert [{"key": "row1", "columns": {"c1": 1, "c2": "2"}}] == jsgz_load(
            mock_raw_row_response.calls[0].request.body
        )["items"]

    def test_insert_single_DTO(self, mock_raw_row_response):
        res = RAW_API.rows.insert(
            db_name="db1", table_name="table1", row=Row(key="row1", columns={"c1": 1, "c2": "2"}), ensure_parent=False
        )
        assert isinstance(res, Row)
        assert mock_raw_row_response.calls[0].response.json()["data"]["items"][0] == res.dump(camel_case=True)
        assert [{"key": "row1", "columns": {"c1": 1, "c2": "2"}}] == jsgz_load(
            mock_raw_row_response.calls[0].request.body
        )["items"]

    def test_insert_multiple_DTO(self, mock_raw_row_response):
        res_list = RAW_API.rows.insert("db1", "table1", row=[Row(key="row1", columns={"c1": 1, "c2": "2"})])
        for res in res_list:
            assert COGNITE_CLIENT == res._client
        assert COGNITE_CLIENT == res_list._client
        assert isinstance(res_list, RowList)
        assert [{"key": "row1", "columns": {"c1": 1, "c2": "2"}}] == jsgz_load(
            mock_raw_row_response.calls[0].request.body
        )["items"]
        assert mock_raw_row_response.calls[0].response.json()["data"]["items"] == res_list.dump(camel_case=True)

    def test_list(self, mock_raw_row_response):
        res_list = RAW_API.rows.list(db_name="db1", table_name="table1")
        assert RowList([Row(key="row1", columns={"c1": 1, "c2": "2"})]) == res_list

    def test_iter_single(self, mock_raw_row_response):
        for db in RAW_API.rows(db_name="db1", table_name="table1"):
            assert mock_raw_row_response.calls[0].response.json()["data"]["items"][0] == db.dump(camel_case=True)

    def test_iter_chunk(self, mock_raw_row_response):
        for db in RAW_API.rows("db1", "table1", chunk_size=1):
            assert mock_raw_row_response.calls[0].response.json()["data"]["items"] == db.dump(camel_case=True)

    def test_delete(self, mock_raw_row_response):
        res = RAW_API.rows.delete("db1", table_name="table1", key="row1")
        assert res is None
        assert [{"key": "row1"}] == jsgz_load(mock_raw_row_response.calls[0].request.body)["items"]

    def test_delete_multiple(self, mock_raw_row_response):
        res = RAW_API.rows.delete(db_name="db1", table_name="table1", key=["row1"])
        assert res is None
        assert [{"key": "row1"}] == jsgz_load(mock_raw_row_response.calls[0].request.body)["items"]
