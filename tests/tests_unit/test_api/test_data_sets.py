import re

import pytest

from cognite.client.data_classes import DataSet, DataSetList, DataSetUpdate, TimestampRange
from tests.utils import jsgz_load


@pytest.fixture
def dataset_update():
    yield DataSetUpdate(id=1).description.set("_")


@pytest.fixture
def mock_ds_response(rsps, cognite_client):
    response_body = {
        "items": [
            {
                "id": 0,
                "externalId": "string",
                "name": "string",
                "description": "string",
                "metadata": {"metadata-key": "metadata-value"},
                "writeProtected": False,
                "createdTime": 0,
                "lastUpdatedTime": 0,
            }
        ]
    }
    url_pattern = re.compile(
        re.escape(cognite_client.data_sets._get_base_url_with_base_path())
        + r"/datasets(?:/byids|/update|/delete|/list|$|\?.+)"
    )
    rsps.assert_all_requests_are_fired = False

    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
    yield rsps


class TestDataset:
    def test_retrieve_single(self, cognite_client, mock_ds_response):
        res = cognite_client.data_sets.retrieve(id=1)
        assert isinstance(res, DataSet)
        assert mock_ds_response.calls[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_retrieve_multiple(self, cognite_client, mock_ds_response):
        res = cognite_client.data_sets.retrieve_multiple(ids=[1])
        assert isinstance(res, DataSetList)
        assert mock_ds_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_list_with_timestamp_range(self, cognite_client, mock_ds_response):
        cognite_client.data_sets.list(created_time=TimestampRange(min=20))
        assert 20 == jsgz_load(mock_ds_response.calls[0].request.body)["filter"]["createdTime"]["min"]
        assert "max" not in jsgz_load(mock_ds_response.calls[0].request.body)["filter"]["createdTime"]

    def test_list_with_time_dict(self, cognite_client, mock_ds_response):
        cognite_client.data_sets.list(last_updated_time={"max": 20})
        assert 20 == jsgz_load(mock_ds_response.calls[0].request.body)["filter"]["lastUpdatedTime"]["max"]
        assert "min" not in jsgz_load(mock_ds_response.calls[0].request.body)["filter"]["lastUpdatedTime"]

    def test_call_root(self, cognite_client, mock_ds_response):
        list(cognite_client.data_sets.__call__(write_protected=True, limit=10))
        calls = mock_ds_response.calls
        assert 1 == len(calls)
        assert {"cursor": None, "limit": 10, "filter": {"writeProtected": True}} == jsgz_load(calls[0].request.body)

    def test_create_single(self, cognite_client, mock_ds_response):
        res = cognite_client.data_sets.create(DataSet(external_id="1", name="blabla"))
        assert isinstance(res, DataSet)
        assert mock_ds_response.calls[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_create_multiple(self, cognite_client, mock_ds_response):
        res = cognite_client.data_sets.create([DataSet(external_id="1")])
        assert isinstance(res, DataSetList)
        assert mock_ds_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_iter_single(self, cognite_client, mock_ds_response):
        for dataset in cognite_client.data_sets:
            assert mock_ds_response.calls[0].response.json()["items"][0] == dataset.dump(camel_case=True)

    @pytest.mark.skip("delete not implemented")
    def test_delete_single(self, cognite_client, mock_ds_response):
        res = cognite_client.data_sets.delete(id=1)
        assert {"items": [{"id": 1}]} == jsgz_load(mock_ds_response.calls[0].request.body)
        assert res is None

    @pytest.mark.skip("delete not implemented")
    def test_delete_multiple(self, cognite_client, mock_ds_response):
        res = cognite_client.data_sets.delete(id=[1])
        assert {"items": [{"id": 1}]} == jsgz_load(mock_ds_response.calls[0].request.body)
        assert res is None

    def test_update_with_resource_class(self, cognite_client, mock_ds_response):
        res = cognite_client.data_sets.update(DataSet(id=1))
        assert isinstance(res, DataSet)
        assert mock_ds_response.calls[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_update_with_update_class(self, cognite_client, mock_ds_response, dataset_update):
        res = cognite_client.data_sets.update(dataset_update)
        assert isinstance(res, DataSet)
        assert mock_ds_response.calls[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_update_multiple(self, cognite_client, mock_ds_response, dataset_update):
        res = cognite_client.data_sets.update([dataset_update])
        assert isinstance(res, DataSetList)
        assert mock_ds_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_event_update_object(self):

        assert isinstance(
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
            .name.set(None),
            DataSetUpdate,
        )


@pytest.fixture
def mock_ds_empty(rsps, cognite_client):
    url_pattern = re.compile(re.escape(cognite_client.data_sets._get_base_url_with_base_path()) + "/.+")
    rsps.add(rsps.POST, url_pattern, status=200, json={"items": []})
    yield rsps


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

        df = cognite_client.data_sets.retrieve(id=1).to_pandas(camel_case=True)
        assert isinstance(df, pd.DataFrame)
        assert "metadata" not in df.columns
        assert df.loc["writeProtected"].bool() is False
        assert "metadata-value" == df.loc["metadata-key"][0]
