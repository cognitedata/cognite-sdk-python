from __future__ import annotations

import math
import re
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

import pytest
from pytest_httpx import HTTPXMock

from cognite.client import CogniteClient
from cognite.client._api.raw.rows import RawRowsAPI
from cognite.client.data_classes import Database, DatabaseList, Row, RowList, RowWrite, RowWriteList, Table, TableList
from cognite.client.data_classes.raw import RowCore, RowListCore
from cognite.client.exceptions import CogniteAPIError
from tests.utils import assert_all_value_types_equal, get_url, jsgz_load

if TYPE_CHECKING:
    from pytest_httpx import HTTPXMock

    from cognite.client import AsyncCogniteClient, CogniteClient


@pytest.fixture
def mock_raw_db_response(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> Iterator[list[dict[str, Any]]]:
    response_body = {"items": [{"name": "db1", "createdTime": 123}]}
    url_pattern = re.compile(re.escape(get_url(async_client.raw)) + r"/raw/dbs(?:/delete|$|\?.+)")

    httpx_mock.add_response(method="POST", url=url_pattern, status_code=200, json=response_body, is_optional=True)
    httpx_mock.add_response(method="GET", url=url_pattern, status_code=200, json=response_body, is_optional=True)
    yield response_body["items"]


@pytest.fixture
def mock_raw_table_response(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> Iterator[list[dict[str, Any]]]:
    response_body = {"items": [{"name": "table1", "createdTime": 123}]}
    url_pattern = re.compile(re.escape(get_url(async_client.raw)) + r"/raw/dbs/db1/tables(?:/delete|$|\?.+)")

    httpx_mock.add_response(method="POST", url=url_pattern, status_code=200, json=response_body, is_optional=True)
    httpx_mock.add_response(method="GET", url=url_pattern, status_code=200, json=response_body, is_optional=True)
    yield response_body["items"]


@pytest.fixture
def example_raw_rows_insert() -> list[dict[str, Any]]:
    return [{"key": "row1", "columns": {"c1": 1, "c2": "2"}}]


@pytest.fixture
def example_raw_rows() -> list[dict[str, Any]]:
    return [{"key": "row1", "columns": {"c1": 1, "c2": "2"}, "lastUpdatedTime": 123}]


@pytest.fixture
def mock_raw_row_response(
    httpx_mock: HTTPXMock,
    cognite_client: CogniteClient,
    example_raw_rows: list[dict[str, Any]],
    async_client: AsyncCogniteClient,
) -> Iterator[list[dict[str, Any]]]:
    response_body = {"items": example_raw_rows}
    raw_path_prefix = re.escape(get_url(async_client.raw)) + "/raw/dbs/db1/tables/table1"
    url_pattern = re.compile(raw_path_prefix + r"/rows(?:/delete|/row1|$|\?.+)")
    cursors_url_pattern = re.compile(raw_path_prefix + "/cursors")

    httpx_mock.add_response(
        method="GET", url=cursors_url_pattern, status_code=200, json=response_body, is_optional=True
    )
    httpx_mock.add_response(method="POST", url=url_pattern, status_code=200, json=response_body, is_optional=True)
    httpx_mock.add_response(method="GET", url=url_pattern, status_code=200, json=response_body, is_optional=True)
    yield response_body["items"]


@pytest.fixture
def mock_retrieve_raw_row_response(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> Iterator[dict[str, Any]]:
    response_body = {"key": "row1", "columns": {"c1": 1, "c2": "2"}, "lastUpdatedTime": 123}
    httpx_mock.add_response(
        method="GET",
        url=get_url(async_client.raw) + "/raw/dbs/db1/tables/table1/rows/row1",
        status_code=200,
        json=response_body,
    )
    yield response_body


@pytest.fixture
def mock_retrieve_raw_rows_response_two_rows(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> Iterator[list[dict[str, Any]]]:
    response_body = {
        "items": [
            {"key": "row1", "columns": {"c1": 1, "c2": "2"}, "lastUpdatedTime": 0},
            {"key": "row2", "columns": {"c1": 2, "c2": "3"}, "lastUpdatedTime": 1},
        ]
    }
    for _ in range(2):
        httpx_mock.add_response(
            method="GET",
            url=get_url(async_client.raw) + "/raw/dbs/db1/tables/table1/rows?limit=25",
            status_code=200,
            json=response_body,
        )
    yield response_body["items"]


@pytest.fixture
def integer_rows_response() -> dict:
    return {
        "items": [
            {"key": "row1", "columns": {"c1": 1, "c2": 4.0}, "lastUpdatedTime": 0},
            {"key": "row2", "columns": {"c1": 2, "c2": 3}, "lastUpdatedTime": 1},
            {"key": "row3", "columns": {"c1": 3, "c2": 1}, "lastUpdatedTime": 2},
            {"key": "row4", "columns": {"c1": None, "c2": 0.1}, "lastUpdatedTime": 3},
        ]
    }


@pytest.fixture
def mock_retrieve_integer_rows(
    httpx_mock: HTTPXMock,
    integer_rows_response: dict[str, Any],
    cognite_client: CogniteClient,
    async_client: AsyncCogniteClient,
) -> Iterator[list[dict[str, Any]]]:
    httpx_mock.add_response(
        method="GET",
        url=get_url(async_client.raw) + "/raw/dbs/db1/tables/table1/rows?limit=25",
        status_code=200,
        json=integer_rows_response,
    )
    yield integer_rows_response["items"]


@pytest.fixture
def mock_retrieve_raw_rows_response_one_row(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> Iterator[list[dict[str, Any]]]:
    response_body = {"items": [{"key": "row1", "columns": {"c1": 1, "c2": "2"}, "lastUpdatedTime": 0}]}
    for _ in range(2):
        httpx_mock.add_response(
            method="GET",
            url=get_url(async_client.raw) + "/raw/dbs/db1/tables/table1/rows?limit=25",
            status_code=200,
            json=response_body,
        )
    yield response_body["items"]


@pytest.fixture
def mock_retrieve_raw_rows_response_no_rows(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> Iterator[list[dict[str, Any]]]:
    for _ in range(2):
        httpx_mock.add_response(
            method="GET",
            url=get_url(async_client.raw) + "/raw/dbs/db1/tables/table1/rows?limit=25",
            status_code=200,
            json={"items": []},
        )
    yield []


class TestRawDatabases:
    def test_create_single(
        self,
        cognite_client: CogniteClient,
        async_client: AsyncCogniteClient,
        mock_raw_db_response: list[dict[str, Any]],
        httpx_mock: HTTPXMock,
    ) -> None:
        res = cognite_client.raw.databases.create(name="db1")
        assert isinstance(res, Database)
        assert async_client == res._cognite_client
        assert mock_raw_db_response[0] == res.dump(camel_case=True)
        assert [{"name": "db1"}] == jsgz_load(httpx_mock.get_requests()[0].content)["items"]

    def test_create_multiple(
        self,
        cognite_client: CogniteClient,
        async_client: AsyncCogniteClient,
        mock_raw_db_response: list[dict[str, Any]],
        httpx_mock: HTTPXMock,
    ) -> None:
        res_list = cognite_client.raw.databases.create(name=["db1"])
        assert isinstance(res_list, DatabaseList)
        for res in res_list:
            assert async_client == res._cognite_client
        assert async_client == res_list._cognite_client
        assert [{"name": "db1"}] == jsgz_load(httpx_mock.get_requests()[0].content)["items"]
        assert mock_raw_db_response == res_list.dump(camel_case=True)

    def test_list(self, cognite_client: CogniteClient, mock_raw_db_response: list[dict[str, Any]]) -> None:
        res_list = cognite_client.raw.databases.list()
        assert DatabaseList([Database("db1", created_time=123)]) == res_list

    def test_iter_chunk(self, cognite_client: CogniteClient, mock_raw_db_response: list[dict[str, Any]]) -> None:
        for db in cognite_client.raw.databases(chunk_size=1):
            assert mock_raw_db_response == db.dump(camel_case=True)

    def test_delete(
        self, cognite_client: CogniteClient, mock_raw_db_response: list[dict[str, Any]], httpx_mock: HTTPXMock
    ) -> None:
        res = cognite_client.raw.databases.delete(name="db1")
        assert res is None
        assert [{"name": "db1"}] == jsgz_load(httpx_mock.get_requests()[0].content)["items"]

    def test_delete_multiple(
        self, cognite_client: CogniteClient, mock_raw_db_response: list[dict[str, Any]], httpx_mock: HTTPXMock
    ) -> None:
        res = cognite_client.raw.databases.delete(name=["db1"])
        assert res is None
        assert [{"name": "db1"}] == jsgz_load(httpx_mock.get_requests()[0].content)["items"]

    def test_delete_fail(
        self, cognite_client: CogniteClient, httpx_mock: HTTPXMock, async_client: AsyncCogniteClient
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=get_url(async_client.raw) + "/raw/dbs/delete",
            status_code=400,
            json={"error": {"message": "User Error", "code": 400}},
        )
        with pytest.raises(CogniteAPIError) as e:
            cognite_client.raw.databases.delete("db1")
        assert e.value.failed == ["db1"]

    def test_get_tables_in_db(
        self,
        cognite_client: CogniteClient,
        mock_raw_db_response: list[dict[str, Any]],
        mock_raw_table_response: list[dict[str, Any]],
    ) -> None:
        db = cognite_client.raw.databases.list()[0]
        tables = db.tables()
        assert TableList([Table(name="table1", created_time=123)]) == tables


class TestRawTables:
    def test_create_single(
        self,
        cognite_client: CogniteClient,
        async_client: AsyncCogniteClient,
        mock_raw_table_response: list[dict[str, Any]],
        httpx_mock: HTTPXMock,
    ) -> None:
        res = cognite_client.raw.tables.create("db1", name="table1")
        assert isinstance(res, Table)
        assert async_client == res._cognite_client
        assert mock_raw_table_response[0] == res.dump(camel_case=True)
        assert [{"name": "table1"}] == jsgz_load(httpx_mock.get_requests()[0].content)["items"]
        assert "db1" == res._db_name

    def test_create_multiple(
        self,
        cognite_client: CogniteClient,
        async_client: AsyncCogniteClient,
        mock_raw_table_response: list[dict[str, Any]],
        httpx_mock: HTTPXMock,
    ) -> None:
        res_list = cognite_client.raw.tables.create("db1", name=["table1"])
        assert isinstance(res_list, TableList)
        for res in res_list:
            assert async_client == res._cognite_client
            assert "db1" == res._db_name
        assert async_client == res_list._cognite_client
        assert [{"name": "table1"}] == jsgz_load(httpx_mock.get_requests()[0].content)["items"]
        assert mock_raw_table_response == res_list.dump(camel_case=True)

    def test_list(
        self,
        cognite_client: CogniteClient,
        async_client: AsyncCogniteClient,
        mock_raw_table_response: list[dict[str, Any]],
        httpx_mock: HTTPXMock,
    ) -> None:
        res_list = cognite_client.raw.tables.list(db_name="db1")
        for res in res_list:
            assert "db1" == res._db_name
            assert async_client == res._cognite_client
        assert TableList([Table("table1", created_time=123)]) == res_list

    def test_iter_chunk(
        self,
        cognite_client: CogniteClient,
        async_client: AsyncCogniteClient,
        mock_raw_table_response: list[dict[str, Any]],
        httpx_mock: HTTPXMock,
    ) -> None:
        for table_list in cognite_client.raw.tables("db1", chunk_size=1):
            for table in table_list:
                assert "db1" == table._db_name
                assert async_client == table._cognite_client
            assert mock_raw_table_response == table_list.dump(camel_case=True)

    def test_delete(
        self, cognite_client: CogniteClient, mock_raw_table_response: list[dict[str, Any]], httpx_mock: HTTPXMock
    ) -> None:
        res = cognite_client.raw.tables.delete("db1", name="table1")
        assert res is None
        assert [{"name": "table1"}] == jsgz_load(httpx_mock.get_requests()[0].content)["items"]

    def test_delete_multiple(
        self, cognite_client: CogniteClient, mock_raw_table_response: list[dict[str, Any]], httpx_mock: HTTPXMock
    ) -> None:
        res = cognite_client.raw.tables.delete(db_name="db1", name=["table1"])
        assert res is None
        assert [{"name": "table1"}] == jsgz_load(httpx_mock.get_requests()[0].content)["items"]

    def test_delete_fail(
        self, cognite_client: CogniteClient, httpx_mock: HTTPXMock, async_client: AsyncCogniteClient
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=get_url(async_client.raw) + "/raw/dbs/db1/tables/delete",
            status_code=400,
            json={"error": {"message": "User Error", "code": 400}},
        )
        with pytest.raises(CogniteAPIError) as e:
            cognite_client.raw.tables.delete("db1", "table1")
        assert e.value.failed == ["table1"]

    def test_get_rows_in_table(
        self,
        cognite_client: CogniteClient,
        mock_raw_table_response: list[dict[str, Any]],
        mock_raw_row_response: HTTPXMock,
        example_raw_rows: list[dict[str, Any]],
    ) -> None:
        tables = cognite_client.raw.tables.list(db_name="db1")
        exp_rows = RowList.load(example_raw_rows)
        assert tables[0].rows() == exp_rows


class TestRawRows:
    def test_retrieve(
        self, cognite_client: CogniteClient, mock_retrieve_raw_row_response: dict[str, Any], httpx_mock: HTTPXMock
    ) -> None:
        res = cognite_client.raw.rows.retrieve(db_name="db1", table_name="table1", key="row1")
        assert res
        assert mock_retrieve_raw_row_response == res.dump(camel_case=True)
        assert str(httpx_mock.get_requests()[0].url).endswith("/rows/row1")

    def test_insert_w_rows_as_dict(
        self,
        cognite_client: CogniteClient,
        mock_raw_row_response: list[dict[str, Any]],
        example_raw_rows_insert: list[dict[str, Any]],
        httpx_mock: HTTPXMock,
    ) -> None:
        res = cognite_client.raw.rows.insert(
            db_name="db1", table_name="table1", row={"row1": {"c1": 1, "c2": "2"}}, ensure_parent=True
        )
        assert res is None
        assert example_raw_rows_insert == jsgz_load(httpx_mock.get_requests()[0].content)["items"]

    def test_insert_single_dto(
        self,
        cognite_client: CogniteClient,
        example_raw_rows_insert: list[dict[str, Any]],
        mock_raw_row_response: list[dict[str, Any]],
        httpx_mock: HTTPXMock,
    ) -> None:
        res = cognite_client.raw.rows.insert(
            db_name="db1",
            table_name="table1",
            row=RowWrite(key="row1", columns={"c1": 1, "c2": "2"}),
            ensure_parent=False,
        )
        assert res is None
        assert example_raw_rows_insert == jsgz_load(httpx_mock.get_requests()[0].content)["items"]

    def test_insert_multiple_dto(
        self,
        cognite_client: CogniteClient,
        example_raw_rows_insert: list[dict[str, Any]],
        mock_raw_row_response: list[dict[str, Any]],
        httpx_mock: HTTPXMock,
    ) -> None:
        res = cognite_client.raw.rows.insert("db1", "table1", row=[RowWrite(key="row1", columns={"c1": 1, "c2": "2"})])
        assert res is None
        assert example_raw_rows_insert == jsgz_load(httpx_mock.get_requests()[0].content)["items"]

    def test_insert_fail(
        self, cognite_client: CogniteClient, httpx_mock: HTTPXMock, async_client: AsyncCogniteClient
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=get_url(async_client.raw) + "/raw/dbs/db1/tables/table1/rows?ensureParent=false",
            status_code=400,
            json={},
        )
        with pytest.raises(CogniteAPIError) as e:
            cognite_client.raw.rows.insert("db1", "table1", {"row1": {"c1": 1}})
        assert e.value.failed == ["row1"]

    def test_list(
        self, cognite_client: CogniteClient, mock_raw_row_response: list[dict[str, Any]], httpx_mock: HTTPXMock
    ) -> None:
        res_list = cognite_client.raw.rows.list(db_name="db1", table_name="table1")
        assert RowList([Row(key="row1", columns={"c1": 1, "c2": "2"}, last_updated_time=123)]) == res_list
        assert b"columns=" not in httpx_mock.get_requests()[0].url.query

    def test_list_cols(
        self, cognite_client: CogniteClient, mock_raw_row_response: list[dict[str, Any]], httpx_mock: HTTPXMock
    ) -> None:
        cognite_client.raw.rows.list(db_name="db1", table_name="table1", columns=["a", "1"])
        assert b"columns=a%2C1" in httpx_mock.get_requests()[0].url.query

    def test_list_cols_empty(
        self, cognite_client: CogniteClient, mock_raw_row_response: list[dict[str, Any]], httpx_mock: HTTPXMock
    ) -> None:
        cognite_client.raw.rows.list(db_name="db1", table_name="table1", columns=[])
        assert b"columns=%2C&" in httpx_mock.get_requests()[0].url.query + b"&"

    def test_list_cols_str_not_supported(
        self, cognite_client: CogniteClient, mock_raw_row_response: list[dict[str, Any]]
    ) -> None:
        with pytest.raises(TypeError):
            cognite_client.raw.rows.list(db_name="db1", table_name="table1", columns="a,b")  # type: ignore[arg-type]

    def test_iter_chunk(
        self, cognite_client: CogniteClient, mock_raw_row_response: list[dict[str, Any]], httpx_mock: HTTPXMock
    ) -> None:
        for db in cognite_client.raw.rows("db1", "table1", chunk_size=1, partitions=None):
            assert mock_raw_row_response == db.dump(camel_case=True)

    def test_delete(
        self, cognite_client: CogniteClient, mock_raw_row_response: list[dict[str, Any]], httpx_mock: HTTPXMock
    ) -> None:
        res = cognite_client.raw.rows.delete("db1", table_name="table1", key="row1")
        assert res is None
        assert [{"key": "row1"}] == jsgz_load(httpx_mock.get_requests()[0].content)["items"]

    def test_delete_multiple(
        self, cognite_client: CogniteClient, mock_raw_row_response: list[dict[str, Any]], httpx_mock: HTTPXMock
    ) -> None:
        res = cognite_client.raw.rows.delete(db_name="db1", table_name="table1", key=["row1"])
        assert res is None
        assert [{"key": "row1"}] == jsgz_load(httpx_mock.get_requests()[0].content)["items"]

    def test_delete_fail(
        self, cognite_client: CogniteClient, httpx_mock: HTTPXMock, async_client: AsyncCogniteClient
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=get_url(async_client.raw) + "/raw/dbs/db1/tables/table1/rows/delete",
            status_code=400,
            json={"error": {"message": "User Error", "code": 400}},
        )
        with pytest.raises(CogniteAPIError) as e:
            cognite_client.raw.rows.delete("db1", "table1", "key1")
        assert e.value.failed == ["key1"]

    def test_iter(
        self, cognite_client: CogniteClient, mock_raw_row_response: list[dict[str, Any]], httpx_mock: HTTPXMock
    ) -> None:
        res_generator = cognite_client.raw.rows(db_name="db1", table_name="table1", chunk_size=None, partitions=None)
        row = next(res_generator)
        assert Row(key="row1", columns={"c1": 1, "c2": "2"}, last_updated_time=123) == row
        assert b"columns=" not in httpx_mock.get_requests()[0].url.query

    def test_iter_cols(
        self, cognite_client: CogniteClient, mock_raw_row_response: list[dict[str, Any]], httpx_mock: HTTPXMock
    ) -> None:
        res_generator = cognite_client.raw.rows(
            db_name="db1", table_name="table1", columns=["a", "1"], chunk_size=None, partitions=None
        )
        next(res_generator)
        assert b"columns=a%2C1" in httpx_mock.get_requests()[0].url.query

    def test_iter_cols_empty(
        self, cognite_client: CogniteClient, mock_raw_row_response: list[dict[str, Any]], httpx_mock: HTTPXMock
    ) -> None:
        res_generator = cognite_client.raw.rows(
            db_name="db1", table_name="table1", columns=[], chunk_size=None, partitions=None
        )
        next(res_generator)
        assert b"columns=%2C&" in httpx_mock.get_requests()[0].url.query + b"&"

    def test_iter_cols_str_not_supported(
        self, cognite_client: CogniteClient, mock_raw_row_response: list[dict[str, Any]]
    ) -> None:
        with pytest.raises(TypeError):
            next(cognite_client.raw.rows(db_name="db1", table_name="table1", columns="a,b"))  # type: ignore[call-overload]


@pytest.mark.dsl
class TestRawRowsDataframe:
    def test_retrieve_dataframe_empty(
        self, cognite_client: CogniteClient, mock_retrieve_raw_rows_response_no_rows: list[dict[str, Any]]
    ) -> None:
        import pandas as pd

        res_df = cognite_client.raw.rows.retrieve_dataframe(db_name="db1", table_name="table1")
        res_df_last_updated_time_in_index = cognite_client.raw.rows.retrieve_dataframe(
            db_name="db1", table_name="table1", last_updated_time_in_index=True
        )
        assert isinstance(res_df, pd.DataFrame)
        assert res_df.shape == (0, 0)
        assert res_df_last_updated_time_in_index.shape == (0, 0)
        assert res_df.equals(res_df_last_updated_time_in_index)

    def test_retrieve_dataframe_one_row(
        self, cognite_client: CogniteClient, mock_retrieve_raw_rows_response_one_row: list[dict[str, Any]]
    ) -> None:
        import pandas as pd

        res_df = cognite_client.raw.rows.retrieve_dataframe(db_name="db1", table_name="table1")
        res_df_last_updated_time_in_index = cognite_client.raw.rows.retrieve_dataframe(
            db_name="db1", table_name="table1", last_updated_time_in_index=True
        )
        assert isinstance(res_df, pd.DataFrame)
        assert res_df.shape == (1, 2)
        assert res_df_last_updated_time_in_index.shape == (1, 2)
        assert res_df.equals(res_df_last_updated_time_in_index.droplevel("last_updated_time"))

    def test_retrieve_dataframe_two_rows(
        self, cognite_client: CogniteClient, mock_retrieve_raw_rows_response_two_rows: list[dict[str, Any]]
    ) -> None:
        import pandas as pd

        res_df = cognite_client.raw.rows.retrieve_dataframe(db_name="db1", table_name="table1")
        res_df_last_updated_time_in_index = cognite_client.raw.rows.retrieve_dataframe(
            db_name="db1", table_name="table1", last_updated_time_in_index=True
        )
        assert isinstance(res_df, pd.DataFrame)
        assert res_df.shape == (2, 2)
        assert res_df_last_updated_time_in_index.shape == (2, 2)
        assert res_df.equals(res_df_last_updated_time_in_index.droplevel("last_updated_time"))
        assert res_df_last_updated_time_in_index.index.names == ["key", "last_updated_time"]
        assert list(res_df_last_updated_time_in_index.index.levels[1]) == [
            pd.Timestamp(0, unit="ms"),
            pd.Timestamp(1, unit="ms"),
        ]

    @pytest.mark.usefixtures("mock_retrieve_integer_rows")
    def test_retrieve_dataframe_integers(
        self,
        cognite_client: CogniteClient,
        mock_retrieve_integer_rows: list[dict[str, Any]],
        integer_rows_response: dict,
    ) -> None:
        result = cognite_client.raw.rows.retrieve_dataframe(
            db_name="db1", table_name="table1", infer_dtypes=False, limit=25
        )
        actual = result.to_dict(orient="index")
        expected = {row["key"]: row["columns"] for row in integer_rows_response["items"]}
        assert_all_value_types_equal(actual, expected)
        assert actual == expected


@pytest.mark.parametrize("raw_cls", (Row, RowWrite))
def test_raw_row__direct_column_access(raw_cls: type[RowCore]) -> None:
    # Verify additional methods: 'get', '__getitem__', '__setitem__', '__delitem__' and '__contains__'
    key = "itsamee"
    if raw_cls is Row:
        row: RowCore = Row(key="foo", columns={"bar": 42, key: "mario"}, last_updated_time=123)
    else:
        row = RowWrite(key="foo", columns={"bar": 42, key: "mario"})
    assert row[key] == row.columns[key] == row.get(key) == "mario"

    row[key] = "luigi?"
    assert row[key] == row.columns[key] == row.get(key) == "luigi?"

    del row[key]
    assert key not in row
    assert key not in row.columns
    assert row.get(key) is None

    row.columns[key] = "wario?"
    assert row[key] == row.columns[key] == "wario?"

    del row.columns[key]
    assert key not in row
    assert key not in row.columns

    del row["bar"]
    assert row.columns == {}
    with pytest.raises(KeyError, match=r"^'wrong-key'$"):
        del row["wrong-key"]

    row.columns = None  # type: ignore[assignment]
    with pytest.raises(RuntimeError, match=r"^columns not set on Row instance$"):
        del row["wrong-key"]


@pytest.mark.dsl
def test_insert_dataframe_raises_on_duplicated_cols(cognite_client: CogniteClient) -> None:
    import pandas as pd

    df = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [1, 2, 3],
            "c": [10, 20, 30],
            "d": [10, 20, 30],
            "e": [100, 200, 300],
            "f": [100, 200, 300],
        }
    )
    df.columns = ["a", "b", "a", "c", "a", "b"]
    with pytest.raises(ValueError, match=r"^Dataframe columns are not unique: \['a', 'b'\]$"):
        cognite_client.raw.rows.insert_dataframe("db", "tbl", df)


@pytest.mark.dsl
def test_df_to_rows_skip_nans() -> None:
    import numpy as np
    import pandas as pd

    df = pd.DataFrame(
        {
            "a": [1, None, 3],
            "b": [1, 2, None],
            "c": [10, 20, 30],
            "d": [math.inf, 20, 30],
            "e": [100, 200, np.nan],
            "f": [None, None, None],
        }
    )
    df.at[1, "f"] = math.nan  # object column, should keep None's, but this should be removed
    res = RawRowsAPI._df_to_rows_skip_nans(df)
    expected = {
        0: {"a": 1.0, "b": 1.0, "c": 10, "d": math.inf, "e": 100.0, "f": None},
        1: {"b": 2.0, "c": 20, "d": 20.0, "e": 200.0},
        2: {"a": 3.0, "c": 30, "d": 30.0, "f": None},
    }
    assert res == expected


@pytest.mark.dsl
class TestPandasIntegration:
    def test_dbs_to_pandas(self) -> None:
        import pandas as pd

        db_list = DatabaseList(
            [Database("kar", created_time=123), Database("car", created_time=123), Database("dar", created_time=123)]
        )

        pd.testing.assert_frame_equal(
            pd.DataFrame({"name": ["kar", "car", "dar"]}), db_list.to_pandas().drop("created_time", axis=1)
        )
        pd.testing.assert_frame_equal(
            pd.DataFrame({"value": ["kar"]}, index=["name"]), db_list[0].to_pandas().drop("created_time")
        )

    def test_tables_to_pandas(self) -> None:
        import pandas as pd

        table_list = TableList(
            [Table("kar", created_time=123), Table("car", created_time=123), Table("dar", created_time=123)]
        )

        pd.testing.assert_frame_equal(
            pd.DataFrame({"name": ["kar", "car", "dar"]}), table_list.to_pandas().drop("created_time", axis=1)
        )
        pd.testing.assert_frame_equal(
            pd.DataFrame({"value": ["kar"]}, index=["name"]), table_list[0].to_pandas().drop("created_time")
        )

    def test_rows_to_pandas(self) -> None:
        import pandas as pd

        row_list = RowList(
            [
                Row("k1", {"c1": "v1", "c2": "v1"}, last_updated_time=123),
                Row("k2", {"c1": "v2", "c2": "v2"}, last_updated_time=123),
            ]
        )
        pd.testing.assert_frame_equal(
            pd.DataFrame({"c1": ["v1", "v2"], "c2": ["v1", "v2"]}, index=["k1", "k2"]),
            row_list.to_pandas().sort_index(axis=1),
        )
        pd.testing.assert_frame_equal(pd.DataFrame({"c1": ["v1"], "c2": ["v1"]}, index=["k1"]), row_list[0].to_pandas())

    def test_rows_to_pandas_missing_cols(self) -> None:
        import pandas as pd

        row_list = RowList(
            [
                Row("k1", {"c1": "v1", "c2": "v1"}, last_updated_time=123),
                Row("k2", {"c1": "v2", "c2": "v2", "c3": "v2"}, last_updated_time=123),
            ]
        )
        pd.testing.assert_frame_equal(
            pd.DataFrame({"c1": ["v1", "v2"], "c2": ["v1", "v2"], "c3": [None, "v2"]}, index=["k1", "k2"]),
            row_list.to_pandas().sort_index(axis=1),
        )
        pd.testing.assert_frame_equal(pd.DataFrame({"c1": ["v1"], "c2": ["v1"]}, index=["k1"]), row_list[0].to_pandas())

    def test_rows_to_pandas__no_rows(self) -> None:
        import pandas as pd

        row_df = RowList([]).to_pandas()
        assert row_df.shape == (0, 0)
        pd.testing.assert_frame_equal(row_df, pd.DataFrame(columns=[], index=[]))

    @pytest.mark.parametrize("lst_cls", (RowList, RowWriteList))
    @pytest.mark.parametrize("n_rows", (1, 5))
    def test_rows_to_pandas__empty_or_sparse(self, lst_cls: type[RowListCore], n_rows: int) -> None:
        # Before version 7.49.2, rows with no column data would be silently dropped when converting to a pandas dataframe,
        # which was most noticable as len(rows) != len(df).
        import pandas as pd

        keys = [f"row-{i}" for i in range(n_rows)]
        if lst_cls is RowList:
            row_list: RowList | RowWriteList = RowList([Row(k, {}, last_updated_time=123) for k in keys])
        else:
            row_list = RowWriteList([RowWrite(k, {}) for k in keys])
        row_df = row_list.to_pandas()
        assert row_df.shape == (n_rows, 0)
        pd.testing.assert_frame_equal(row_df, pd.DataFrame(columns=[], index=keys))

        row_list[0]["foo"] = 123
        row_df = row_list.to_pandas()
        assert row_df.shape == (n_rows, 1)
        pd.testing.assert_frame_equal(
            row_df, pd.DataFrame({"foo": [123, *[None for _ in range(n_rows - 1)]]}, index=keys)
        )
