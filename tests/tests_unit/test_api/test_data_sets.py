import re

import pytest

from cognite.client.data_classes import DataSet, DataSetList, DataSetUpdate, TimestampRange
from tests.utils import get_url, jsgz_load


@pytest.fixture
def example_data_set():
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
def dataset_update():
    yield DataSetUpdate(id=1).description.set("_")


@pytest.fixture
def mock_ds_response(httpx_mock, cognite_client, example_data_set):
    response_body = {"items": [example_data_set]}
    url_pattern = re.compile(
        re.escape(get_url(cognite_client.data_sets)) + r"/datasets(?:/byids|/update|/delete|/list|$|\?.+)"
    )

    httpx_mock.add_response(method="POST", url=url_pattern, status_code=200, json=response_body, is_optional=True)
    httpx_mock.add_response(method="GET", url=url_pattern, status_code=200, json=response_body, is_optional=True)
    yield httpx_mock


class TestDataset:
    def test_retrieve_single(self, cognite_client, mock_ds_response, example_data_set):
        res = cognite_client.data_sets.retrieve(id=1)
        assert isinstance(res, DataSet)
        assert example_data_set == res.dump(camel_case=True)

    def test_retrieve_multiple(self, cognite_client, mock_ds_response, example_data_set):
        res = cognite_client.data_sets.retrieve_multiple(ids=[1])
        assert isinstance(res, DataSetList)
        assert [example_data_set] == res.dump(camel_case=True)

    def test_list_with_timestamp_range(self, cognite_client, mock_ds_response):
        cognite_client.data_sets.list(created_time=TimestampRange(min=20))
        assert 20 == jsgz_load(mock_ds_response.get_requests()[0].content)["filter"]["createdTime"]["min"]
        assert "max" not in jsgz_load(mock_ds_response.get_requests()[0].content)["filter"]["createdTime"]

    def test_list_with_time_dict(self, cognite_client, mock_ds_response):
        cognite_client.data_sets.list(last_updated_time={"max": 20})
        assert 20 == jsgz_load(mock_ds_response.get_requests()[0].content)["filter"]["lastUpdatedTime"]["max"]
        assert "min" not in jsgz_load(mock_ds_response.get_requests()[0].content)["filter"]["lastUpdatedTime"]

    def test_call_root(self, cognite_client, mock_ds_response):
        list(cognite_client.data_sets(write_protected=True, limit=10))
        calls = mock_ds_response.get_requests()
        assert 1 == len(calls)
        assert {"limit": 10, "filter": {"writeProtected": True}} == jsgz_load(calls[0].content)

    def test_create_single(self, cognite_client, mock_ds_response, example_data_set):
        res = cognite_client.data_sets.create(DataSet(external_id="1", name="blabla"))
        assert isinstance(res, DataSet)
        assert example_data_set == res.dump(camel_case=True)

    def test_create_multiple(self, cognite_client, mock_ds_response, example_data_set):
        res = cognite_client.data_sets.create([DataSet(external_id="1")])
        assert isinstance(res, DataSetList)
        assert [example_data_set] == res.dump(camel_case=True)

    def test_iter_single(self, cognite_client, mock_ds_response, example_data_set):
        for dataset in cognite_client.data_sets:
            assert example_data_set == dataset.dump(camel_case=True)

    @pytest.mark.skip("delete not implemented")
    def test_delete_single(self, cognite_client, mock_ds_response):
        res = cognite_client.data_sets.delete(id=1)
        assert {"items": [{"id": 1}]} == jsgz_load(mock_ds_response.get_requests()[0].content)
        assert res is None

    @pytest.mark.skip("delete not implemented")
    def test_delete_multiple(self, cognite_client, mock_ds_response):
        res = cognite_client.data_sets.delete(id=[1])
        assert {"items": [{"id": 1}]} == jsgz_load(mock_ds_response.get_requests()[0].content)
        assert res is None

    def test_update_with_resource_class(self, cognite_client, mock_ds_response, example_data_set):
        res = cognite_client.data_sets.update(DataSet(id=1))
        assert isinstance(res, DataSet)
        assert example_data_set == res.dump(camel_case=True)

    def test_update_with_update_class(self, cognite_client, mock_ds_response, dataset_update, example_data_set):
        res = cognite_client.data_sets.update(dataset_update)
        assert isinstance(res, DataSet)
        assert example_data_set == res.dump(camel_case=True)

    def test_update_multiple(self, cognite_client, mock_ds_response, dataset_update, example_data_set):
        res = cognite_client.data_sets.update([dataset_update])
        assert isinstance(res, DataSetList)
        assert [example_data_set] == res.dump(camel_case=True)

    def test_event_update_object(self):
        update = (
            DataSetUpdate(1)
            .description.set("")
            .description.set(None)
            .external_id.set("")
            .external_id.set(None)
            .metadata.set({"_": "."})
            .metadata.set(None)
            .write_protected.set(False)
            .write_protected.set(None)
            .name.set("")
            .name.set(None)
        )
        assert isinstance(update, DataSetUpdate)


@pytest.fixture
def mock_ds_empty(httpx_mock, cognite_client):
    url_pattern = re.compile(re.escape(get_url(cognite_client.data_sets)) + "/.+")
    httpx_mock.add_response(method="POST", url=url_pattern, status_code=200, json={"items": []})
    yield httpx_mock


@pytest.mark.dsl
class TestPandasIntegration:
    def test_datasets_list_to_pandas(self, cognite_client, mock_ds_response):
        import pandas as pd

        df = cognite_client.data_sets.list().to_pandas()
        assert isinstance(df, pd.DataFrame)
        assert 1 == df.shape[0]
        assert {"metadata-key": "metadata-value"} == df["metadata"][0]

    def test_datasets_list_to_pandas_empty(self, cognite_client, mock_ds_empty):
        import pandas as pd

        df = cognite_client.data_sets.list().to_pandas()
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_datasets_to_pandas(self, cognite_client, mock_ds_response):
        import pandas as pd

        df = cognite_client.data_sets.retrieve(id=1).to_pandas(
            expand_metadata=True, metadata_prefix="", camel_case=True
        )
        assert isinstance(df, pd.DataFrame)
        assert "metadata" not in df.columns
        assert df.at["writeProtected", "value"] is False
        assert "metadata-value" == df.at["metadata-key", "value"]
