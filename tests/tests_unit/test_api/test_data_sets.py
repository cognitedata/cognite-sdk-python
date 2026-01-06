from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any
from datetime import datetime, timezone

import pytest
from pytest_httpx import HTTPXMock

from cognite.client import CogniteClient
from cognite.client.data_classes import DataSet, DataSetList, DataSetUpdate, DataSetWrite, TimestampRange
from tests.utils import get_or_raise, get_url, jsgz_load

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient, CogniteClient


@pytest.fixture
def example_data_set() -> dict[str, Any]:
    return {
        "id": 0,
        "externalId": "string",
        "name": "string",
        "description": "string",
        "metadata": {"metadata-key": "metadata-value"},
        "writeProtected": False,
        "createdTime": 0,
        "lastUpdatedTime": 0,
    }


@pytest.fixture
def dataset_update() -> DataSetUpdate:
    return DataSetUpdate(id=1).description.set("_")


@pytest.fixture
def mock_ds_response(
    httpx_mock: HTTPXMock,
    cognite_client: CogniteClient,
    example_data_set: dict[str, Any],
    async_client: AsyncCogniteClient,
) -> HTTPXMock:
    response_body = {"items": [example_data_set]}
    url_pattern = re.compile(
        re.escape(get_url(async_client.data_sets)) + r"/datasets(?:/byids|/update|/delete|/list|$|\?.+)"
    )

    httpx_mock.add_response(method="POST", url=url_pattern, status_code=200, json=response_body, is_optional=True)
    httpx_mock.add_response(method="GET", url=url_pattern, status_code=200, json=response_body, is_optional=True)
    return httpx_mock


class TestDataset:
    def test_retrieve_single(
        self, cognite_client: CogniteClient, mock_ds_response: HTTPXMock, example_data_set: dict[str, Any]
    ) -> None:
        res = cognite_client.data_sets.retrieve(id=1)
        assert isinstance(res, DataSet)
        assert example_data_set == res.dump(camel_case=True)

    def test_retrieve_multiple(
        self, cognite_client: CogniteClient, mock_ds_response: HTTPXMock, example_data_set: dict[str, Any]
    ) -> None:
        res = cognite_client.data_sets.retrieve_multiple(ids=[1])
        assert isinstance(res, DataSetList)
        assert [example_data_set] == res.dump(camel_case=True)

    @pytest.mark.parametrize("min_time", [20, datetime.fromtimestamp(20 / 1000, timezone.utc)])
    def test_list_with_timestamp_range(
        self, cognite_client: CogniteClient, mock_ds_response: HTTPXMock, min_time: int | datetime
    ) -> None:
        cognite_client.data_sets.list(created_time=TimestampRange(min=min_time))
        assert 20 == jsgz_load(mock_ds_response.calls[0].request.body)["filter"]["createdTime"]["min"]
        assert "max" not in jsgz_load(mock_ds_response.calls[0].request.body)["filter"]["createdTime"]

    def test_list_with_time_dict(self, cognite_client: CogniteClient, mock_ds_response: HTTPXMock) -> None:
        cognite_client.data_sets.list(last_updated_time={"max": 20})
        assert 20 == jsgz_load(mock_ds_response.get_requests()[0].content)["filter"]["lastUpdatedTime"]["max"]
        assert "min" not in jsgz_load(mock_ds_response.get_requests()[0].content)["filter"]["lastUpdatedTime"]

    def test_call_root(self, cognite_client: CogniteClient, mock_ds_response: HTTPXMock) -> None:
        list(cognite_client.data_sets(chunk_size=None, write_protected=True, limit=10))
        calls = mock_ds_response.get_requests()
        assert 1 == len(calls)
        assert {"limit": 10, "filter": {"writeProtected": True}} == jsgz_load(calls[0].content)

    def test_create_single(
        self, cognite_client: CogniteClient, mock_ds_response: HTTPXMock, example_data_set: dict[str, Any]
    ) -> None:
        res = cognite_client.data_sets.create(DataSetWrite(external_id="1", name="blabla"))
        assert isinstance(res, DataSet)
        assert example_data_set == res.dump(camel_case=True)

    def test_create_multiple(
        self, cognite_client: CogniteClient, mock_ds_response: HTTPXMock, example_data_set: dict[str, Any]
    ) -> None:
        res = cognite_client.data_sets.create([DataSetWrite(external_id="1")])
        assert isinstance(res, DataSetList)
        assert [example_data_set] == res.dump(camel_case=True)

    @pytest.mark.skip("delete not implemented")
    def test_delete_single(self, cognite_client: CogniteClient, mock_ds_response: HTTPXMock) -> None:
        res = cognite_client.data_sets.delete(id=1)  # type: ignore[attr-defined]
        assert {"items": [{"id": 1}]} == jsgz_load(mock_ds_response.get_requests()[0].content)
        assert res is None

    @pytest.mark.skip("delete not implemented")
    def test_delete_multiple(self, cognite_client: CogniteClient, mock_ds_response: HTTPXMock) -> None:
        res = cognite_client.data_sets.delete(id=[1])  # type: ignore[attr-defined]
        assert {"items": [{"id": 1}]} == jsgz_load(mock_ds_response.get_requests()[0].content)
        assert res is None

    def test_update_with_resource_class(
        self, cognite_client: CogniteClient, mock_ds_response: HTTPXMock, example_data_set: dict[str, Any]
    ) -> None:
        res = cognite_client.data_sets.update(
            DataSet(
                id=1,
                created_time=123,
                last_updated_time=123,
                name=None,
                write_protected=False,
                external_id=None,
                description=None,
                metadata=None,
            )
        )
        assert isinstance(res, DataSet)
        assert example_data_set == res.dump(camel_case=True)

    def test_update_with_update_class(
        self,
        cognite_client: CogniteClient,
        mock_ds_response: HTTPXMock,
        dataset_update: DataSetUpdate,
        example_data_set: dict[str, Any],
    ) -> None:
        res = cognite_client.data_sets.update(dataset_update)
        assert isinstance(res, DataSet)
        assert example_data_set == res.dump(camel_case=True)

    def test_update_multiple(
        self,
        cognite_client: CogniteClient,
        mock_ds_response: HTTPXMock,
        dataset_update: DataSetUpdate,
        example_data_set: dict[str, Any],
    ) -> None:
        res = cognite_client.data_sets.update([dataset_update])
        assert isinstance(res, DataSetList)
        assert [example_data_set] == res.dump(camel_case=True)

    def test_event_update_object(self, async_client: AsyncCogniteClient) -> None:
        update = (
            DataSetUpdate(1)
            .description.set("")
            .description.set(None)
            .external_id.set("")
            .external_id.set(None)
            .metadata.set({"_": "."})
            .metadata.set({})
            .write_protected.set(False)
            .write_protected.set(None)
            .name.set("")
            .name.set(None)
        )
        assert isinstance(update, DataSetUpdate)


@pytest.fixture
def mock_ds_empty(httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient) -> HTTPXMock:
    url_pattern = re.compile(re.escape(get_url(async_client.data_sets)) + "/.+")
    httpx_mock.add_response(method="POST", url=url_pattern, status_code=200, json={"items": []})
    return httpx_mock


@pytest.mark.dsl
class TestPandasIntegration:
    def test_datasets_list_to_pandas(self, cognite_client: CogniteClient, mock_ds_response: HTTPXMock) -> None:
        import pandas as pd

        df = cognite_client.data_sets.list().to_pandas()
        assert isinstance(df, pd.DataFrame)
        assert 1 == df.shape[0]
        assert {"metadata-key": "metadata-value"} == df["metadata"][0]

    def test_datasets_list_to_pandas_empty(self, cognite_client: CogniteClient, mock_ds_empty: HTTPXMock) -> None:
        import pandas as pd

        df = cognite_client.data_sets.list().to_pandas()
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_datasets_to_pandas(self, cognite_client: CogniteClient, mock_ds_response: HTTPXMock) -> None:
        import pandas as pd

        seq = get_or_raise(cognite_client.data_sets.retrieve(id=1))
        df = seq.to_pandas(expand_metadata=True, metadata_prefix="", camel_case=True)
        assert isinstance(df, pd.DataFrame)
        assert "metadata" not in df.columns
        assert df.at["writeProtected", "value"] is False
        assert "metadata-value" == df.at["metadata-key", "value"]
