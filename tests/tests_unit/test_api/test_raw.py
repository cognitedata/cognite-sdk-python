import re

import pytest

from cognite.client import CogniteClient
from cognite.client._api.raw import Database, DatabaseList, Row, RowList, Table, TableList
from cognite.client.exceptions import CogniteAPIError
from tests.utils import jsgz_load

COGNITE_CLIENT = CogniteClient()
RAW_API = COGNITE_CLIENT.raw


@pytest.fixture
def mock_raw_db_response(rsps):
    response_body = {"items": [{"name": "db1"}]}

    url_pattern = re.compile(re.escape(RAW_API._get_base_url_with_base_path()) + "/raw/dbs(?:/delete|$|\?.+)")
    rsps.assert_all_requests_are_fired = False

    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
    yield rsps


@pytest.fixture
def mock_raw_table_response(rsps):
    response_body = {"items": [{"name": "table1"}]}

    url_pattern = re.compile(
        re.escape(RAW_API._get_base_url_with_base_path()) + "/raw/dbs/db1/tables(?:/delete|$|\?.+)"
    )
    rsps.assert_all_requests_are_fired = False

    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
    yield rsps


@pytest.fixture
def mock_raw_row_response(rsps):
    response_body = {"items": [{"key": "row1", "columns": {"c1": 1, "c2": "2"}}]}

    raw_path_prefix = re.escape(RAW_API._get_base_url_with_base_path()) + "/raw/dbs/db1/tables/table1"
    url_pattern = re.compile(raw_path_prefix + "/rows(?:/delete|/row1|$|\?.+)")
    cursors_url_pattern = re.compile(raw_path_prefix + "/cursors")
    rsps.assert_all_requests_are_fired = False

    rsps.add(rsps.GET, cursors_url_pattern, status=200, json=response_body)
    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
    yield rsps


@pytest.fixture
def mock_retrieve_raw_row_response(rsps):
    response_body = {"key": "row1", "columns": {"c1": 1, "c2": "2"}}
    rsps.add(
        rsps.GET,
        RAW_API._get_base_url_with_base_path() + "/raw/dbs/db1/tables/table1/rows/row1",
        status=200,
        json=response_body,
    )
    yield rsps


class TestRawDatabases:
    def test_create_single(self, mock_raw_db_response):
        res = RAW_API.databases.create(name="db1")
        assert isinstance(res, Database)
        assert COGNITE_CLIENT == res._cognite_client
        assert mock_raw_db_response.calls[0].response.json()["items"][0] == res.dump(camel_case=True)
        assert [{"name": "db1"}] == jsgz_load(mock_raw_db_response.calls[0].request.body)["items"]

    def test_create_multiple(self, mock_raw_db_response):
        res_list = RAW_API.databases.create(name=["db1"])
        assert isinstance(res_list, DatabaseList)
        for res in res_list:
            assert COGNITE_CLIENT == res._cognite_client
        assert COGNITE_CLIENT == res_list._cognite_client
        assert [{"name": "db1"}] == jsgz_load(mock_raw_db_response.calls[0].request.body)["items"]
        assert mock_raw_db_response.calls[0].response.json()["items"] == res_list.dump(camel_case=True)

    def test_list(self, mock_raw_db_response):
        res_list = RAW_API.databases.list()
        assert DatabaseList([Database("db1")]) == res_list

    def test_iter_single(self, mock_raw_db_response):
        for db in RAW_API.databases:
            assert mock_raw_db_response.calls[0].response.json()["items"][0] == db.dump(camel_case=True)

    def test_iter_chunk(self, mock_raw_db_response):
        for db in RAW_API.databases(chunk_size=1):
            assert mock_raw_db_response.calls[0].response.json()["items"] == db.dump(camel_case=True)

    def test_delete(self, mock_raw_db_response):
        res = RAW_API.databases.delete(name="db1")
        assert res is None
        assert [{"name": "db1"}] == jsgz_load(mock_raw_db_response.calls[0].request.body)["items"]

    def test_delete_multiple(self, mock_raw_db_response):
        res = RAW_API.databases.delete(name=["db1"])
        assert res is None
        assert [{"name": "db1"}] == jsgz_load(mock_raw_db_response.calls[0].request.body)["items"]

    def test_delete_fail(self, rsps):
        rsps.add(
            rsps.POST,
            RAW_API._get_base_url_with_base_path() + "/raw/dbs/delete",
            status=400,
            json={"error": {"message": "User Error", "code": 400}},
        )
        with pytest.raises(CogniteAPIError) as e:
            RAW_API.databases.delete("db1")
        assert e.value.failed == ["db1"]

    def test_get_tables_in_db(self, mock_raw_db_response, mock_raw_table_response):
        db = RAW_API.databases.list()[0]
        tables = db.tables()
        assert TableList([Table(name="table1")]) == tables


class TestRawTables:
    def test_create_single(self, mock_raw_table_response):
        res = RAW_API.tables.create("db1", name="table1")
        assert isinstance(res, Table)
        assert COGNITE_CLIENT == res._cognite_client
        assert mock_raw_table_response.calls[0].response.json()["items"][0] == res.dump(camel_case=True)
        assert [{"name": "table1"}] == jsgz_load(mock_raw_table_response.calls[0].request.body)["items"]
        assert "db1" == res._db_name

    def test_create_multiple(self, mock_raw_table_response):
        res_list = RAW_API.tables.create("db1", name=["table1"])
        assert isinstance(res_list, TableList)
        for res in res_list:
            assert COGNITE_CLIENT == res._cognite_client
            assert "db1" == res._db_name
        assert COGNITE_CLIENT == res_list._cognite_client
        assert [{"name": "table1"}] == jsgz_load(mock_raw_table_response.calls[0].request.body)["items"]
        assert mock_raw_table_response.calls[0].response.json()["items"] == res_list.dump(camel_case=True)

    def test_list(self, mock_raw_table_response):
        res_list = RAW_API.tables.list(db_name="db1")
        for res in res_list:
            assert "db1" == res._db_name
            assert COGNITE_CLIENT == res._cognite_client
        assert TableList([Table("table1")]) == res_list

    def test_iter_single(self, mock_raw_table_response):
        for table in RAW_API.tables(db_name="db1"):
            assert mock_raw_table_response.calls[0].response.json()["items"][0] == table.dump(camel_case=True)

    def test_iter_chunk(self, mock_raw_table_response):
        for table_list in RAW_API.tables("db1", chunk_size=1):
            for table in table_list:
                assert "db1" == table._db_name
                assert COGNITE_CLIENT == table._cognite_client
            assert mock_raw_table_response.calls[0].response.json()["items"] == table_list.dump(camel_case=True)

    def test_delete(self, mock_raw_table_response):
        res = RAW_API.tables.delete("db1", name="table1")
        assert res is None
        assert [{"name": "table1"}] == jsgz_load(mock_raw_table_response.calls[0].request.body)["items"]

    def test_delete_multiple(self, mock_raw_table_response):
        res = RAW_API.tables.delete(db_name="db1", name=["table1"])
        assert res is None
        assert [{"name": "table1"}] == jsgz_load(mock_raw_table_response.calls[0].request.body)["items"]

    def test_delete_fail(self, rsps):
        rsps.add(
            rsps.POST,
            RAW_API._get_base_url_with_base_path() + "/raw/dbs/db1/tables/delete",
            status=400,
            json={"error": {"message": "User Error", "code": 400}},
        )
        with pytest.raises(CogniteAPIError) as e:
            RAW_API.tables.delete("db1", "table1")
        assert e.value.failed == ["table1"]

    def test_get_rows_in_table(self, mock_raw_table_response, mock_raw_row_response):
        tables = RAW_API.tables.list(db_name="db1")
        rows = tables[0].rows()
        assert RowList([Row._load({"key": "row1", "columns": {"c1": 1, "c2": "2"}})]) == rows


class TestRawRows:
    def test_retrieve(self, mock_retrieve_raw_row_response):
        res = RAW_API.rows.retrieve(db_name="db1", table_name="table1", key="row1")
        assert mock_retrieve_raw_row_response.calls[0].response.json() == res.dump(camel_case=True)
        assert mock_retrieve_raw_row_response.calls[0].request.url.endswith("/rows/row1")

    def test_insert_w_rows_as_dict(self, mock_raw_row_response):
        res = RAW_API.rows.insert(
            db_name="db1", table_name="table1", row={"row1": {"c1": 1, "c2": "2"}}, ensure_parent=True
        )
        assert res is None
        assert [{"key": "row1", "columns": {"c1": 1, "c2": "2"}}] == jsgz_load(
            mock_raw_row_response.calls[0].request.body
        )["items"]

    def test_insert_single_DTO(self, mock_raw_row_response):
        res = RAW_API.rows.insert(
            db_name="db1", table_name="table1", row=Row(key="row1", columns={"c1": 1, "c2": "2"}), ensure_parent=False
        )
        assert res is None
        assert [{"key": "row1", "columns": {"c1": 1, "c2": "2"}}] == jsgz_load(
            mock_raw_row_response.calls[0].request.body
        )["items"]

    def test_insert_multiple_DTO(self, mock_raw_row_response):
        res = RAW_API.rows.insert("db1", "table1", row=[Row(key="row1", columns={"c1": 1, "c2": "2"})])
        assert res is None
        assert [{"key": "row1", "columns": {"c1": 1, "c2": "2"}}] == jsgz_load(
            mock_raw_row_response.calls[0].request.body
        )["items"]

    def test_insert_fail(self, rsps):
        rsps.add(
            rsps.POST, RAW_API._get_base_url_with_base_path() + "/raw/dbs/db1/tables/table1/rows", status=400, json={}
        )
        with pytest.raises(CogniteAPIError) as e:
            RAW_API.rows.insert("db1", "table1", {"row1": {"c1": 1}})
        assert e.value.failed == ["row1"]

    def test_list(self, mock_raw_row_response):
        res_list = RAW_API.rows.list(db_name="db1", table_name="table1")
        assert RowList([Row(key="row1", columns={"c1": 1, "c2": "2"})]) == res_list
        assert "columns=" not in mock_raw_row_response.calls[0].request.path_url

    def test_list_cols(self, mock_raw_row_response):
        RAW_API.rows.list(db_name="db1", table_name="table1", columns=["a", 1])
        assert "columns=a%2C1" in mock_raw_row_response.calls[0].request.path_url

    def test_list_cols_empty(self, mock_raw_row_response):
        RAW_API.rows.list(db_name="db1", table_name="table1", columns=[])
        assert "columns=%2C&" in mock_raw_row_response.calls[0].request.path_url + "&"

    def test_list_cols_str_not_supported(self, mock_raw_row_response):
        with pytest.raises(ValueError):
            RAW_API.rows.list(db_name="db1", table_name="table1", columns="a,b")

    def test_iter_single(self, mock_raw_row_response):
        for db in RAW_API.rows(db_name="db1", table_name="table1"):
            assert mock_raw_row_response.calls[0].response.json()["items"][0] == db.dump(camel_case=True)

    def test_iter_chunk(self, mock_raw_row_response):
        for db in RAW_API.rows("db1", "table1", chunk_size=1):
            assert mock_raw_row_response.calls[0].response.json()["items"] == db.dump(camel_case=True)

    def test_delete(self, mock_raw_row_response):
        res = RAW_API.rows.delete("db1", table_name="table1", key="row1")
        assert res is None
        assert [{"key": "row1"}] == jsgz_load(mock_raw_row_response.calls[0].request.body)["items"]

    def test_delete_multiple(self, mock_raw_row_response):
        res = RAW_API.rows.delete(db_name="db1", table_name="table1", key=["row1"])
        assert res is None
        assert [{"key": "row1"}] == jsgz_load(mock_raw_row_response.calls[0].request.body)["items"]

    def test_delete_fail(self, rsps):
        rsps.add(
            rsps.POST,
            RAW_API._get_base_url_with_base_path() + "/raw/dbs/db1/tables/table1/rows/delete",
            status=400,
            json={"error": {"message": "User Error", "code": 400}},
        )
        with pytest.raises(CogniteAPIError) as e:
            RAW_API.rows.delete("db1", "table1", "key1")
        assert e.value.failed == ["key1"]

    def test_iter(self, mock_raw_row_response):
        res_generator = RAW_API.rows(db_name="db1", table_name="table1")
        row = next(res_generator)
        assert Row(key="row1", columns={"c1": 1, "c2": "2"}) == row
        assert "columns=" not in mock_raw_row_response.calls[0].request.path_url

    def test_iter_cols(self, mock_raw_row_response):
        res_generator = RAW_API.rows(db_name="db1", table_name="table1", columns=["a", 1])
        next(res_generator)
        assert "columns=a%2C1" in mock_raw_row_response.calls[0].request.path_url

    def test_iter_cols_empty(self, mock_raw_row_response):
        res_generator = RAW_API.rows(db_name="db1", table_name="table1", columns=[])
        next(res_generator)
        assert "columns=%2C&" in mock_raw_row_response.calls[0].request.path_url + "&"

    def test_iter_cols_str_not_supported(self, mock_raw_row_response):
        with pytest.raises(ValueError):
            RAW_API.rows(db_name="db1", table_name="table1", columns="a,b")


@pytest.mark.dsl
class TestPandasIntegration:
    def test_dbs_to_pandas(self):
        import pandas as pd

        db_list = DatabaseList([Database("kar"), Database("car"), Database("dar")])

        pd.testing.assert_frame_equal(pd.DataFrame({"name": ["kar", "car", "dar"]}), db_list.to_pandas())
        pd.testing.assert_frame_equal(pd.DataFrame({"value": ["kar"]}, index=["name"]), db_list[0].to_pandas())

    def test_tables_to_pandas(self):
        import pandas as pd

        table_list = TableList([Table("kar"), Table("car"), Table("dar")])

        pd.testing.assert_frame_equal(pd.DataFrame({"name": ["kar", "car", "dar"]}), table_list.to_pandas())
        pd.testing.assert_frame_equal(pd.DataFrame({"value": ["kar"]}, index=["name"]), table_list[0].to_pandas())

    def test_rows_to_pandas(self):
        import pandas as pd

        row_list = RowList([Row("k1", {"c1": "v1", "c2": "v1"}), Row("k2", {"c1": "v2", "c2": "v2"})])
        pd.testing.assert_frame_equal(
            pd.DataFrame({"c1": ["v1", "v2"], "c2": ["v1", "v2"]}, index=["k1", "k2"]),
            row_list.to_pandas().sort_index(axis=1),
        )
        pd.testing.assert_frame_equal(pd.DataFrame({"c1": ["v1"], "c2": ["v1"]}, index=["k1"]), row_list[0].to_pandas())

    def test_rows_to_pandas_missing_cols(self):
        import pandas as pd

        row_list = RowList([Row("k1", {"c1": "v1", "c2": "v1"}), Row("k2", {"c1": "v2", "c2": "v2", "c3": "v2"})])
        pd.testing.assert_frame_equal(
            pd.DataFrame({"c1": ["v1", "v2"], "c2": ["v1", "v2"], "c3": [None, "v2"]}, index=["k1", "k2"]),
            row_list.to_pandas().sort_index(axis=1),
        )
        pd.testing.assert_frame_equal(pd.DataFrame({"c1": ["v1"], "c2": ["v1"]}, index=["k1"]), row_list[0].to_pandas())
