from __future__ import annotations

import re

import pytest

from cognite.client.data_classes import (
    Sequence,
    SequenceColumnWrite,
    SequenceFilter,
    SequenceList,
    SequenceRows,
    SequenceRowsList,
    SequenceUpdate,
    SequenceWrite,
)
from tests.utils import get_url, jsgz_load


@pytest.fixture
def mock_seq_response(httpx_mock, cognite_client):
    response_body = {
        "items": [
            {
                "id": 0,
                "externalId": "string",
                "name": "stringname",
                "metadata": {"metadata-key": "metadata-value"},
                "assetId": 0,
                "description": "string",
                "createdTime": 0,
                "lastUpdatedTime": 0,
                "columns": [
                    {
                        "externalId": "column1",
                        "valueType": "STRING",
                        "metadata": {"column-metadata-key": "column-metadata-value"},
                        "createdTime": 0,
                        "lastUpdatedTime": 0,
                    },
                    {
                        "externalId": "column2",
                        "valueType": "DOUBLE",
                        "metadata": {"column-metadata-key": "column-metadata-value"},
                        "createdTime": 0,
                        "lastUpdatedTime": 0,
                    },
                ],
            }
        ]
    }
    url_pattern = re.compile(
        re.escape(get_url(cognite_client.sequences)) + r"/sequences(?:/byids|/list|/update|/delete|/search|$|\?.+)"
    )
    httpx_mock.add_response(
        method="POST", url=url_pattern, status_code=200, json=response_body, is_optional=True, is_reusable=True
    )
    httpx_mock.add_response(method="GET", url=url_pattern, status_code=200, json=response_body, is_optional=True)
    yield response_body


@pytest.fixture
def mock_sequences_empty(httpx_mock, cognite_client):
    response_body = {"items": []}
    url_pattern = re.compile(
        re.escape(get_url(cognite_client.sequences)) + r"/sequences(?:/byids|/update|/list|/delete|/search|$|\?.+)"
    )
    httpx_mock.add_response(method="POST", url=url_pattern, status_code=200, json=response_body, is_optional=True)
    httpx_mock.add_response(method="GET", url=url_pattern, status_code=200, json=response_body, is_optional=True)
    yield httpx_mock


@pytest.fixture
def mock_post_sequence_data(httpx_mock, cognite_client):
    httpx_mock.add_response(
        method="POST",
        url=get_url(cognite_client.sequences) + "/sequences/data",
        status_code=200,
        json={},
        is_reusable=True,
    )
    yield httpx_mock


@pytest.fixture
def mock_get_sequence_data(httpx_mock, cognite_client):
    payload = {
        "id": 0,
        "externalId": "eid",
        "columns": [{"externalId": "ceid", "createdTime": 123, "lastUpdatedTime": 123, "valueType": "Double"}],
        "rows": [{"rowNumber": 0, "values": [1]}],
    }
    httpx_mock.add_response(
        method="POST",
        url=get_url(cognite_client.sequences) + "/sequences/data/list",
        status_code=200,
        json=payload,
    )
    yield payload


@pytest.fixture
def mock_get_sequence_empty_data(httpx_mock, cognite_client):
    json = {
        "id": 0,
        "externalId": "eid",
        "columns": [
            {"externalId": "ceid", "createdTime": 123, "lastUpdatedTime": 123, "valueType": "String"},
            {"externalId": "ceid2", "createdTime": 123, "lastUpdatedTime": 123, "valueType": "String"},
        ],
        "rows": [],
    }
    httpx_mock.add_response(
        method="POST",
        url=get_url(cognite_client.sequences) + "/sequences/data/list",
        status_code=200,
        json=json,
    )
    yield httpx_mock


@pytest.fixture
def mock_get_sequence_data_many_columns(httpx_mock, cognite_client):
    json = {
        "id": 0,
        "externalId": "eid",
        "columns": [
            {"externalId": f"ceid{i}", "createdTime": 123, "lastUpdatedTime": 123, "valueType": "String"}
            for i in range(200)
        ],
        "rows": [{"rowNumber": 0, "values": ["str"] * 200}],
    }
    httpx_mock.add_response(
        method="POST",
        url=get_url(cognite_client.sequences) + "/sequences/data/list",
        status_code=200,
        json=json,
    )
    yield httpx_mock


@pytest.fixture
def mock_get_sequence_data_two_col(httpx_mock, cognite_client):
    json = {
        "id": 0,
        "externalId": "eid",
        "columns": [
            {"externalId": "col1", "createdTime": 123, "lastUpdatedTime": 123, "valueType": "Long"},
            {"externalId": "col2", "createdTime": 123, "lastUpdatedTime": 123, "valueType": "Long"},
        ],
        "rows": [{"rowNumber": 0, "values": [1, 2]}],
    }
    httpx_mock.add_response(
        method="POST",
        url=get_url(cognite_client.sequences) + "/sequences/data/list",
        status_code=200,
        json=json,
        is_reusable=True,
    )
    yield httpx_mock


@pytest.fixture
def mock_get_sequence_data_two_col_with_zero(httpx_mock, cognite_client):
    json = {
        "id": 0,
        "externalId": "eid",
        "columns": [
            {"externalId": "str", "createdTime": 123, "lastUpdatedTime": 123, "valueType": "String"},
            {"externalId": "lon", "createdTime": 123, "lastUpdatedTime": 123, "valueType": "Long"},
        ],
        "rows": [{"rowNumber": 12, "values": ["string-12", 0]}],
    }
    httpx_mock.add_response(
        method="POST",
        url=get_url(cognite_client.sequences) + "/sequences/data/list",
        status_code=200,
        json=json,
        is_reusable=True,
    )
    yield httpx_mock


@pytest.fixture
def mock_get_sequence_data_with_null(httpx_mock, cognite_client):
    json = {
        "id": 0,
        "externalId": "eid",
        "columns": [
            {"id": 0, "externalId": "intcol", "createdTime": 123, "lastUpdatedTime": 123, "valueType": "Long"},
            {"id": 1, "externalId": "strcol", "createdTime": 123, "lastUpdatedTime": 123, "valueType": "Long"},
        ],
        "rows": [{"rowNumber": 0, "values": [1, None]}, {"rowNumber": 1, "values": [None, "blah"]}],
    }
    httpx_mock.add_response(
        method="POST",
        url=get_url(cognite_client.sequences) + "/sequences/data/list",
        status_code=200,
        json=json,
    )
    yield httpx_mock


@pytest.fixture
def mock_delete_sequence_data(httpx_mock, cognite_client):
    httpx_mock.add_response(
        method="POST",
        url=get_url(cognite_client.sequences) + "/sequences/data/delete",
        status_code=200,
        json={},
    )
    yield httpx_mock


class TestSequences:
    def test_retrieve_single(self, cognite_client, mock_seq_response):
        res = cognite_client.sequences.retrieve(id=1)
        assert isinstance(res, Sequence)
        assert mock_seq_response["items"][0] == res.dump(camel_case=True)

    def test_column_ids(self, cognite_client, mock_seq_response):
        seq = cognite_client.sequences.retrieve(id=1)
        assert isinstance(seq, Sequence)
        assert ["column1", "column2"] == seq.column_external_ids
        assert ["STRING", "DOUBLE"] == seq.column_value_types

    def test_retrieve_multiple(self, cognite_client, mock_seq_response):
        res = cognite_client.sequences.retrieve_multiple(ids=[1])
        assert isinstance(res, SequenceList)
        assert mock_seq_response["items"] == res.dump(camel_case=True)

    def test_list(self, cognite_client, mock_seq_response):
        res = cognite_client.sequences.list()
        assert mock_seq_response["items"] == res.dump(camel_case=True)

    def test_list_with_filters(self, cognite_client, mock_seq_response, httpx_mock):
        res = cognite_client.sequences.list(
            metadata={"a": "b"},
            last_updated_time={"min": 45},
            created_time={"max": 123},
            asset_ids=[1, 2],
            data_set_ids=[11],
            data_set_external_ids=["fml"],
            asset_subtree_ids=[1],
            asset_subtree_external_ids=["a"],
        )
        assert mock_seq_response["items"] == res.dump(camel_case=True)
        assert {
            "metadata": {"a": "b"},
            "assetIds": [1, 2],
            "assetSubtreeIds": [{"id": 1}, {"externalId": "a"}],
            "createdTime": {"max": 123},
            "lastUpdatedTime": {"min": 45},
            "dataSetIds": [{"id": 11}, {"externalId": "fml"}],
        } == jsgz_load(httpx_mock.get_requests()[0].content)["filter"]

    def test_create_single(self, cognite_client, mock_seq_response, httpx_mock):
        res = cognite_client.sequences.create(
            SequenceWrite(external_id="1", name="blabla", columns=[SequenceColumnWrite(external_id="column0")])
        )
        assert isinstance(res, Sequence)
        assert mock_seq_response["items"][0] == res.dump(camel_case=True)
        assert {
            "items": [
                {"name": "blabla", "externalId": "1", "columns": [{"externalId": "column0", "valueType": "Double"}]}
            ]
        } == jsgz_load(httpx_mock.get_requests()[0].content)

    def test_create_single_multicol(self, cognite_client, mock_seq_response, httpx_mock):
        res = cognite_client.sequences.create(
            SequenceWrite(
                external_id="1",
                name="blabla",
                columns=[
                    SequenceColumnWrite(value_type="String", external_id="column0"),
                    SequenceColumnWrite(external_id="c2"),
                ],
            )
        )
        assert isinstance(res, Sequence)
        assert mock_seq_response["items"][0] == res.dump(camel_case=True)
        assert {
            "items": [
                {
                    "name": "blabla",
                    "externalId": "1",
                    "columns": [
                        {"externalId": "column0", "valueType": "String"},
                        {"externalId": "c2", "valueType": "Double"},
                    ],
                }
            ]
        } == jsgz_load(httpx_mock.get_requests()[0].content)

    def test_create_columnid_passed(self, cognite_client, mock_seq_response, httpx_mock):
        res = cognite_client.sequences.create(
            SequenceWrite(
                external_id="1", name="blabla", columns=[SequenceColumnWrite(external_id="a", value_type="String")]
            )
        )
        assert isinstance(res, Sequence)
        assert {
            "items": [{"name": "blabla", "externalId": "1", "columns": [{"valueType": "String", "externalId": "a"}]}]
        } == jsgz_load(httpx_mock.get_requests()[0].content)

    def test_create_multiple(self, cognite_client, mock_seq_response):
        res = cognite_client.sequences.create(
            [SequenceWrite(external_id="1", name="blabla", columns=[SequenceColumnWrite(external_id="cid")])]
        )
        assert isinstance(res, SequenceList)
        assert mock_seq_response["items"] == res.dump(camel_case=True)

    def test_iter_single(self, cognite_client, mock_seq_response):
        for asset in cognite_client.sequences:
            assert mock_seq_response["items"][0] == asset.dump(camel_case=True)

    def test_iter_chunk(self, cognite_client, mock_seq_response):
        for assets in cognite_client.sequences(chunk_size=1):
            assert mock_seq_response["items"] == assets.dump(camel_case=True)

    def test_delete_single(self, cognite_client, mock_seq_response, httpx_mock):
        res = cognite_client.sequences.delete(id=1)
        assert {"ignoreUnknownIds": False, "items": [{"id": 1}]} == jsgz_load(httpx_mock.get_requests()[0].content)
        assert res is None

    def test_delete_multiple(self, cognite_client, mock_seq_response, httpx_mock):
        res = cognite_client.sequences.delete(id=[1])
        assert {"ignoreUnknownIds": False, "items": [{"id": 1}]} == jsgz_load(httpx_mock.get_requests()[0].content)
        assert res is None

    def test_update_with_resource_class(self, cognite_client, mock_seq_response):
        res = cognite_client.sequences.update(
            Sequence(
                id=1,
                created_time=123,
                last_updated_time=123,
                name=None,
                description=None,
                asset_id=None,
                external_id=None,
                metadata=None,
                columns=[],
                data_set_id=None,
                cognite_client=None,
            )
        )
        assert isinstance(res, Sequence)
        assert mock_seq_response["items"][0] == res.dump(camel_case=True)

    def test_update_with_update_class(self, cognite_client, mock_seq_response):
        res = cognite_client.sequences.update(SequenceUpdate(id=1).description.set("blabla"))
        assert isinstance(res, Sequence)
        assert mock_seq_response["items"][0] == res.dump(camel_case=True)

    def test_update_multiple(self, cognite_client, mock_seq_response):
        res = cognite_client.sequences.update([SequenceUpdate(id=1).description.set("blabla")])
        assert isinstance(res, SequenceList)
        assert mock_seq_response["items"] == res.dump(camel_case=True)

    def test_search(self, cognite_client, mock_seq_response, httpx_mock):
        res = cognite_client.sequences.search(filter=SequenceFilter(external_id_prefix="e"))
        assert mock_seq_response["items"] == res.dump(camel_case=True)
        assert {
            "search": {"name": None, "description": None, "query": None},
            "filter": {"externalIdPrefix": "e"},
            "limit": 25,
        } == jsgz_load(httpx_mock.get_requests()[0].content)

    @pytest.mark.parametrize("filter_field", ["is_string", "isString"])
    def test_search_dict_filter(self, cognite_client, mock_seq_response, filter_field, httpx_mock):
        res = cognite_client.sequences.search(filter={filter_field: True})
        assert mock_seq_response["items"] == res.dump(camel_case=True)
        assert {
            "search": {"name": None, "description": None, "query": None},
            "filter": {"isString": True},
            "limit": 25,
        } == jsgz_load(httpx_mock.get_requests()[0].content)

    def test_search_with_filter(self, cognite_client, mock_seq_response, httpx_mock):
        res = cognite_client.sequences.search(
            name="n", description="d", query="q", filter=SequenceFilter(last_updated_time={"max": 42})
        )
        assert mock_seq_response["items"] == res.dump(camel_case=True)
        req_body = jsgz_load(httpx_mock.get_requests()[0].content)
        assert 42 == req_body["filter"]["lastUpdatedTime"]["max"]
        assert {"name": "n", "description": "d", "query": "q"} == req_body["search"]

    def test_update_object(self):
        assert isinstance(
            SequenceUpdate(1)
            .asset_id.set(1)
            .asset_id.set(None)
            .description.set("")
            .description.set(None)
            .external_id.set("1")
            .external_id.set(None)
            .metadata.set({})
            .name.set("")
            .name.set(None),
            SequenceUpdate,
        )

    def test_insert(self, cognite_client, mock_post_sequence_data):
        data = {i: [2 * i] for i in range(1, 11)}
        cognite_client.sequences.data.insert(column_external_ids=["0"], rows=data, external_id="eid")
        assert {
            "items": [
                {
                    "externalId": "eid",
                    "columns": ["0"],
                    "rows": [{"rowNumber": i, "values": [2 * i]} for i in range(1, 11)],
                }
            ]
        } == jsgz_load(mock_post_sequence_data.get_requests()[0].content)

    def test_insert_extid(self, cognite_client, mock_post_sequence_data):
        data = {i: [2 * i] for i in range(1, 11)}
        cognite_client.sequences.data.insert(column_external_ids=["blah"], rows=data, external_id="eid")
        assert {
            "items": [
                {
                    "externalId": "eid",
                    "columns": ["blah"],
                    "rows": [{"rowNumber": i, "values": [2 * i]} for i in range(1, 11)],
                }
            ]
        } == jsgz_load(mock_post_sequence_data.get_requests()[0].content)

    def test_insert_tuple(self, cognite_client, mock_post_sequence_data):
        data = [(i, [2 * i]) for i in range(1, 11)]
        cognite_client.sequences.data.insert(column_external_ids=["col0"], rows=data, external_id="eid")
        assert {
            "items": [
                {
                    "externalId": "eid",
                    "columns": ["col0"],
                    "rows": [{"rowNumber": i, "values": [2 * i]} for i in range(1, 11)],
                }
            ]
        } == jsgz_load(mock_post_sequence_data.get_requests()[0].content)

    def test_insert_raw(self, cognite_client, mock_post_sequence_data):
        data = [{"rowNumber": i, "values": [2 * i, "str"]} for i in range(1, 11)]
        cognite_client.sequences.data.insert(column_external_ids=["c0"], rows=data, external_id="eid")
        assert {
            "items": [
                {
                    "externalId": "eid",
                    "columns": ["c0"],
                    "rows": [{"rowNumber": i, "values": [2 * i, "str"]} for i in range(1, 11)],
                }
            ]
        } == jsgz_load(mock_post_sequence_data.get_requests()[0].content)

    def test_retrieve_no_data(self, cognite_client, mock_seq_response, mock_get_sequence_empty_data):
        data = cognite_client.sequences.rows.retrieve(id=1, start=0, end=None)
        assert isinstance(data, SequenceRows)
        assert 0 == len(data)
        assert 2 == len(data.column_external_ids)

    def test_retrieve_multi_data(self, cognite_client, mock_seq_response, mock_get_sequence_data_two_col_with_zero):
        data = cognite_client.sequences.data.retrieve(id=[1, 2], start=0, end=None)
        assert isinstance(data, SequenceRowsList)
        assert 2 == len(data)
        assert 2 == len(data[0].column_external_ids)
        assert 2 == len(data[1].column_external_ids)

    def test_retrieve_multi_data_mixed(
        self, cognite_client, mock_seq_response, mock_get_sequence_data_two_col_with_zero
    ):
        data = cognite_client.sequences.data.retrieve(external_id="a", id=[1, 2])
        assert isinstance(data, SequenceRowsList)
        assert 3 == len(data)
        assert 2 == len(data[0].column_external_ids)
        assert 2 == len(data[1].column_external_ids)
        assert 2 == len(data[2].column_external_ids)

    def test_retrieve_by_id(self, cognite_client, mock_seq_response, mock_get_sequence_data):
        data = cognite_client.sequences.data.retrieve(id=123, start=123, end=None)
        assert isinstance(data, SequenceRows)
        assert 1 == len(data)

    def test_retrieve_by_external_id(self, cognite_client, mock_seq_response, mock_get_sequence_data):
        data = cognite_client.sequences.data.retrieve(external_id="foo", start=1, end=1000)
        assert isinstance(data, SequenceRows)
        assert 1 == len(data)

    def test_retrieve_missing_column_external_id(
        self, cognite_client, mock_seq_response, mock_get_sequence_data_two_col
    ):
        data = cognite_client.sequences.data.retrieve(id=123, start=123, end=None)
        assert isinstance(data, SequenceRows)
        assert 1 == len(data)

    def test_retrieve_neg_1_end_index(self, cognite_client, mock_seq_response, mock_get_sequence_data, httpx_mock):
        cognite_client.sequences.data.retrieve(id=123, start=123, end=-1)
        assert jsgz_load(httpx_mock.get_requests()[0].content)["end"] is None

    def test_delete_by_id(self, cognite_client, mock_delete_sequence_data):
        res = cognite_client.sequences.data.delete(id=1, rows=[1, 2, 3])
        assert res is None
        assert {"items": [{"id": 1, "rows": [1, 2, 3]}]} == jsgz_load(
            mock_delete_sequence_data.get_requests()[0].content
        )

    def test_delete_by_external_id(self, cognite_client, mock_delete_sequence_data):
        res = cognite_client.sequences.data.delete(external_id="foo", rows=[1])
        assert res is None
        assert {"items": [{"externalId": "foo", "rows": [1]}]} == jsgz_load(
            mock_delete_sequence_data.get_requests()[0].content
        )

    def test_retrieve_rows(self, cognite_client, mock_seq_response, mock_get_sequence_data):
        seq = cognite_client.sequences.retrieve(id=1)
        data = seq.rows(start=0, end=None)
        assert isinstance(data, SequenceRows)
        assert 1 == len(data)

    def test_rows_helper_functions(self, cognite_client, mock_seq_response, mock_get_sequence_data):
        seq = cognite_client.sequences.retrieve(id=1)
        data = seq.rows(start=0, end=None)
        assert [1] == data[0]
        for r, v in data.items():
            assert 0 == r
            assert [1] == v
        col = data.get_column("ceid")
        assert isinstance(col, list)
        assert 1 == len(col)
        with pytest.raises(TypeError):
            data[1:23]
        with pytest.raises(ValueError):
            data.get_column("doesnotexist")

    def test_sequence_builtins(self, cognite_client, mock_seq_response):
        r1 = cognite_client.sequences.retrieve(id=0)
        r2 = cognite_client.sequences.retrieve(id=0)
        assert r1 == r2
        assert r1.__eq__(r2)
        assert str(r1) == str(r2)
        assert mock_seq_response["items"][0] == r1.dump(camel_case=True)


@pytest.mark.dsl
class TestSequencesPandasIntegration:
    def test_retrieve_dataframe(self, cognite_client, mock_seq_response, mock_get_sequence_data):
        df = cognite_client.sequences.data.retrieve(external_id="foo", start=1000000, end=1100000).to_pandas()
        assert {"ceid"} == set(df.columns)
        assert df.shape[0] > 0
        assert df.index == [0]

    def test_retrieve_dataframe_columns_mixed(self, cognite_client, mock_seq_response, mock_get_sequence_data_two_col):
        data = cognite_client.sequences.data.retrieve(external_id="foo", start=1000000, end=1100000)
        assert isinstance(data, SequenceRows)
        assert ["col1", "col2"] == list(data.to_pandas().columns)

    def test_retrieve_dataframe_multi(self, cognite_client, mock_seq_response, mock_get_sequence_data_two_col):
        data = cognite_client.sequences.data.retrieve(external_id="foo", id=2, start=1000000, end=1100000)
        assert isinstance(data, SequenceRowsList)
        assert ["col1", "col2", "col1", "col2"] == list(
            data.to_pandas(column_names="columnExternalId", concat=True).columns
        )

    def test_to_pandas_invalid(self, cognite_client, mock_seq_response, mock_get_sequence_data_two_col):
        with pytest.raises(ValueError):
            cognite_client.sequences.data.retrieve(external_id="foo", id=2, start=1000000, end=1000001).to_pandas(
                column_names="id~columnExternalId"
            )
        with pytest.raises(ValueError):
            cognite_client.sequences.data.retrieve(external_id="foo", id=2, start=1000000, end=1000001).to_pandas(
                column_names="columnExternalIds"
            )

    def test_retrieve_dataframe_column_names(self, cognite_client, mock_seq_response, mock_get_sequence_data_two_col):
        data = cognite_client.sequences.data.retrieve(external_id=["eid", "eid"], start=1000000, end=1100000)
        assert isinstance(data, SequenceRowsList)
        assert ["col1", "col2", "col1", "col2"] == list(
            data.to_pandas(column_names="columnExternalId", concat=True).columns
        )
        assert ["eid", "eid", "eid", "eid"] == list(data.to_pandas(column_names="externalId", concat=True).columns)
        assert ["eid|col1", "eid|col2", "eid|col1", "eid|col2"] == list(data.to_pandas(concat=True).columns)

    def test_retrieve_dataframe_columns_mixed_with_zero(
        self, cognite_client, mock_seq_response, mock_get_sequence_data_two_col_with_zero
    ):
        import pandas as pd

        data = cognite_client.sequences.data.retrieve_dataframe(external_id="foo", start=0, end=100)
        expected_df = pd.DataFrame(index=[12], data=[["string-12", 0]], columns=["str", "lon"]).astype(
            {"str": pd.StringDtype(), "lon": pd.Int64Dtype()}
        )
        pd.testing.assert_frame_equal(expected_df, data)

    def test_retrieve_dataframe_columns_filter(
        self, cognite_client, mock_seq_response, mock_get_sequence_data_two_col_with_zero
    ):
        data_1 = cognite_client.sequences.data.retrieve_dataframe(
            external_id="foo", start=0, end=100, column_external_ids=["ceid1"]
        )
        data_2 = cognite_client.sequences.data.retrieve(
            external_id="foo", start=0, end=100, columns=["ceid1"]
        ).to_pandas()
        assert data_1.equals(data_2)

    def test_retrieve_dataframe_columns_many_extid(self, cognite_client, mock_get_sequence_data_many_columns):
        data = cognite_client.sequences.data.retrieve(external_id="foo", start=1000000, end=1100000)
        assert isinstance(data, SequenceRows)
        assert [f"ceid{i}" for i in range(200)] == list(data.to_pandas().columns)

    def test_retrieve_dataframe_convert_null(self, cognite_client, mock_seq_response, mock_get_sequence_data_with_null):
        df = cognite_client.sequences.data.retrieve(external_id="foo", start=0, end=None).to_pandas()
        assert {"strcol", "intcol"} == set(df.columns)
        assert "None" not in df.strcol
        assert df.strcol.isna().any()
        assert df.intcol.isna().any()
        assert 2 == df.shape[0]

    def test_retrieve_empty_dataframe(self, cognite_client, mock_seq_response, mock_get_sequence_empty_data):
        import pandas as pd

        df = cognite_client.sequences.data.retrieve(id=1, start=0, end=None).to_pandas()
        assert isinstance(df, pd.DataFrame)
        assert df.empty
        assert 2 == len(df.columns)

    def test_sequences_list_to_pandas(self, cognite_client, mock_seq_response):
        import pandas as pd

        df = cognite_client.sequences.list().to_pandas()
        assert isinstance(df, pd.DataFrame)
        assert 1 == df.shape[0]
        assert {"metadata-key": "metadata-value"} == df["metadata"][0]

    def test_sequences_list_to_pandas_empty(self, cognite_client, mock_sequences_empty):
        import pandas as pd

        df = cognite_client.sequences.list().to_pandas()
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_sequences_to_pandas(self, cognite_client, mock_seq_response):
        import pandas as pd

        df = cognite_client.sequences.retrieve(id=1).to_pandas(expand_metadata=True, metadata_prefix="")
        assert isinstance(df, pd.DataFrame)
        assert "metadata" not in df.columns
        assert "string" == df.loc["description"][0]
        assert "metadata-value" == df.loc["metadata-key"][0]

    def test_insert_dataframe_extids(self, cognite_client, mock_post_sequence_data):
        import pandas as pd

        df = pd.DataFrame(index=[123, 456])
        df["aa"] = [1, 2]
        df["bb"] = [5.0, 6.0]
        res = cognite_client.sequences.data.insert_dataframe(df, id=42)
        assert res is None
        request_body = jsgz_load(mock_post_sequence_data.get_requests()[0].content)
        expected_rows = [{"rowNumber": 123, "values": [1, 5.0]}, {"rowNumber": 456, "values": [2, 6.0]}]
        expected_body = {"items": [{"id": 42, "columns": ["aa", "bb"], "rows": expected_rows}]}
        assert expected_body == request_body

    def test_insert_dataframe_with_missing_values(self, cognite_client, mock_post_sequence_data):
        import pandas as pd

        # Both missing third element + col A missing last + col B missing first
        df = pd.DataFrame(
            {
                "col-A": [1.0, 2.0, None, 4.0, None],
                "col-B": [None, "b", None, "d", "e"],
            },
            index=range(5),
        )
        cognite_client.sequences.data.insert_dataframe(df, id=42)
        request_body = jsgz_load(mock_post_sequence_data.get_requests()[0].content)
        expected_rows = [
            {"rowNumber": 0, "values": [1.0, None]},
            {"rowNumber": 1, "values": [2.0, "b"]},
            {"rowNumber": 3, "values": [4.0, "d"]},
            {"rowNumber": 4, "values": [None, "e"]},
        ]
        expected_body = {"items": [{"id": 42, "columns": ["col-A", "col-B"], "rows": expected_rows}]}
        assert expected_body == request_body

        cognite_client.sequences.data.insert_dataframe(df, id=42, dropna=False)
        request_body = jsgz_load(mock_post_sequence_data.get_requests()[1].content)
        expected_rows.insert(2, {"rowNumber": 2, "values": [None, None]})
        assert expected_body == request_body
