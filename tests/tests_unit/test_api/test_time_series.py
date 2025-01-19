import re

import pytest

from cognite.client.data_classes import TimeSeries, TimeSeriesFilter, TimeSeriesList, TimeSeriesUpdate
from tests.utils import get_url, jsgz_load


@pytest.fixture
def mock_ts_response(httpx_mock, cognite_client):
    response_body = {
        "items": [
            {
                "id": 0,
                "externalId": "string",
                "name": "stringname",
                "isString": True,
                "metadata": {"metadata-key": "metadata-value"},
                "unit": "string",
                "assetId": 0,
                "isStep": True,
                "description": "string",
                "securityCategories": [0],
                "createdTime": 0,
                "lastUpdatedTime": 0,
            }
        ]
    }
    url_pattern = re.compile(
        re.escape(get_url(cognite_client.time_series)) + r"/timeseries(?:/byids|/update|/delete|/list|/search|$|\?.+)"
    )
    # ....assert_all_requests_are_fired = False  # TODO

    httpx_mock.add_response(method="POST", url=url_pattern, status_code=200, json=response_body)
    httpx_mock.add_response(method="GET", url=url_pattern, status_code=200, json=response_body)
    yield httpx_mock


class TestTimeSeries:
    def test_retrieve_single(self, cognite_client, mock_ts_response):
        res = cognite_client.time_series.retrieve(id=1)
        assert isinstance(res, TimeSeries)
        assert mock_ts_response.get_requests()[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_retrieve_multiple(self, cognite_client, mock_ts_response):
        res = cognite_client.time_series.retrieve_multiple(ids=[1])
        assert isinstance(res, TimeSeriesList)
        assert mock_ts_response.get_requests()[0].response.json()["items"] == res.dump(camel_case=True)

    def test_list(self, cognite_client, mock_ts_response):
        res = cognite_client.time_series.list()
        assert mock_ts_response.get_requests()[0].response.json()["items"] == res.dump(camel_case=True)

    def test_list_with_filters(self, cognite_client, mock_ts_response):
        res = cognite_client.time_series.list(
            is_step=True,
            is_string=False,
            metadata={"a": "b"},
            last_updated_time={"min": 45},
            created_time={"max": 123},
            asset_ids=[1, 2],
            asset_external_ids=["aeid"],
            data_set_ids=[1, 2],
            data_set_external_ids=["x"],
            asset_subtree_ids=[1],
            asset_subtree_external_ids=["a"],
        )
        assert mock_ts_response.get_requests()[0].response.json()["items"] == res.dump(camel_case=True)
        assert {
            "isString": False,
            "isStep": True,
            "metadata": {"a": "b"},
            "assetIds": [1, 2],
            "assetExternalIds": ["aeid"],
            "assetSubtreeIds": [{"id": 1}, {"externalId": "a"}],
            "dataSetIds": [{"id": 1}, {"id": 2}, {"externalId": "x"}],
            "createdTime": {"max": 123},
            "lastUpdatedTime": {"min": 45},
        } == jsgz_load(mock_ts_response.get_requests()[0].content)["filter"]

    @pytest.mark.dsl
    def test_list_with_asset_ids(self, cognite_client, mock_ts_response):
        import numpy

        cognite_client.time_series.list(asset_ids=[1])
        cognite_client.time_series.list(asset_ids=[numpy.int64(1)])
        for i in range(len(mock_ts_response.get_requests())):
            assert [1] == jsgz_load(mock_ts_response.get_requests()[i].content)["filter"]["assetIds"]

    def test_create_single(self, cognite_client, mock_ts_response):
        res = cognite_client.time_series.create(TimeSeries(external_id="1", name="blabla"))
        assert isinstance(res, TimeSeries)
        assert mock_ts_response.get_requests()[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_create_multiple(self, cognite_client, mock_ts_response):
        res = cognite_client.time_series.create([TimeSeries(external_id="1", name="blabla")])
        assert isinstance(res, TimeSeriesList)
        assert mock_ts_response.get_requests()[0].response.json()["items"] == res.dump(camel_case=True)

    def test_iter_single(self, cognite_client, mock_ts_response):
        for asset in cognite_client.time_series:
            assert mock_ts_response.get_requests()[0].response.json()["items"][0] == asset.dump(camel_case=True)

    def test_iter_chunk(self, cognite_client, mock_ts_response):
        for assets in cognite_client.time_series(chunk_size=1):
            assert mock_ts_response.get_requests()[0].response.json()["items"] == assets.dump(camel_case=True)

    def test_delete_single(self, cognite_client, mock_ts_response):
        res = cognite_client.time_series.delete(id=1)
        assert {"items": [{"id": 1}], "ignoreUnknownIds": False} == jsgz_load(
            mock_ts_response.get_requests()[0].content
        )
        assert res is None

    def test_delete_single_ignore_unknown(self, cognite_client, mock_ts_response):
        res = cognite_client.time_series.delete(id=1, ignore_unknown_ids=True)
        assert {"items": [{"id": 1}], "ignoreUnknownIds": True} == jsgz_load(mock_ts_response.get_requests()[0].content)
        assert res is None

    def test_delete_multiple(self, cognite_client, mock_ts_response):
        res = cognite_client.time_series.delete(id=[1])
        assert {"items": [{"id": 1}], "ignoreUnknownIds": False} == jsgz_load(
            mock_ts_response.get_requests()[0].content
        )
        assert res is None

    def test_update_with_resource_class(self, cognite_client, mock_ts_response):
        res = cognite_client.time_series.update(TimeSeries(id=1))
        assert isinstance(res, TimeSeries)
        assert mock_ts_response.get_requests()[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_update_with_update_class(self, cognite_client, mock_ts_response):
        res = cognite_client.time_series.update(TimeSeriesUpdate(id=1).description.set("blabla"))
        assert isinstance(res, TimeSeries)
        assert mock_ts_response.get_requests()[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_update_multiple(self, cognite_client, mock_ts_response):
        res = cognite_client.time_series.update([TimeSeriesUpdate(id=1).description.set("blabla")])
        assert isinstance(res, TimeSeriesList)
        assert mock_ts_response.get_requests()[0].response.json()["items"] == res.dump(camel_case=True)

    def test_update_multiple_list(self, cognite_client, mock_ts_response):
        tsl = cognite_client.time_series.retrieve_multiple(ids=[0])
        res = cognite_client.time_series.update(tsl)
        assert isinstance(res, TimeSeriesList)
        assert 1 == len(res)

    def test_search(self, cognite_client, mock_ts_response):
        res = cognite_client.time_series.search(filter=TimeSeriesFilter(is_string=True))
        assert mock_ts_response.get_requests()[0].response.json()["items"] == res.dump(camel_case=True)
        assert {
            "search": {"name": None, "description": None, "query": None},
            "filter": {"isString": True},
            "limit": 25,
        } == jsgz_load(mock_ts_response.get_requests()[0].content)

    @pytest.mark.parametrize("filter_field", ["is_string", "isString"])
    def test_search_dict_filter(self, cognite_client, mock_ts_response, filter_field):
        res = cognite_client.time_series.search(filter={filter_field: True})
        assert mock_ts_response.get_requests()[0].response.json()["items"] == res.dump(camel_case=True)
        assert {
            "search": {"name": None, "description": None, "query": None},
            "filter": {"isString": True},
            "limit": 25,
        } == jsgz_load(mock_ts_response.get_requests()[0].content)

    def test_search_with_filter(self, cognite_client, mock_ts_response):
        res = cognite_client.time_series.search(
            name="n", description="d", query="q", filter=TimeSeriesFilter(unit="bla")
        )
        assert mock_ts_response.get_requests()[0].response.json()["items"] == res.dump(camel_case=True)
        req_body = jsgz_load(mock_ts_response.get_requests()[0].content)
        assert "bla" == req_body["filter"]["unit"]
        assert {"name": "n", "description": "d", "query": "q"} == req_body["search"]

    def test_update_object(self):
        assert isinstance(
            TimeSeriesUpdate(1)
            .asset_id.set(1)
            .asset_id.set(None)
            .description.set("")
            .description.set(None)
            .external_id.set("1")
            .external_id.set(None)
            .metadata.set({})
            .name.set("")
            .name.set(None)
            .security_categories.set([])
            .unit.set("")
            .unit.set(None),
            TimeSeriesUpdate,
        )
        assert isinstance(
            TimeSeriesUpdate(1)
            .metadata.add({})
            .metadata.remove([])
            .security_categories.add([])
            .security_categories.remove([]),
            TimeSeriesUpdate,
        )


@pytest.fixture
def mock_time_series_empty(httpx_mock, cognite_client):
    url_pattern = re.compile(re.escape(get_url(cognite_client.time_series)) + "/.+")
    # ....assert_all_requests_are_fired = False  # TODO
    httpx_mock.add_response(method="POST", url=url_pattern, status_code=200, json={"items": []})
    httpx_mock.add_response(method="GET", url=url_pattern, status_code=200, json={"items": []})
    yield httpx_mock


@pytest.mark.dsl
class TestPandasIntegration:
    def test_time_series_list_to_pandas(self, cognite_client, mock_ts_response):
        import pandas as pd

        df = cognite_client.time_series.list().to_pandas()
        assert isinstance(df, pd.DataFrame)
        assert 1 == df.shape[0]
        assert {"metadata-key": "metadata-value"} == df["metadata"][0]

    def test_time_series_list_to_pandas_empty(self, cognite_client, mock_time_series_empty):
        import pandas as pd

        df = cognite_client.time_series.list().to_pandas()
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_time_series_to_pandas(self, cognite_client, mock_ts_response):
        import pandas as pd

        df = cognite_client.time_series.retrieve(id=1).to_pandas(
            expand_metadata=True, metadata_prefix="", camel_case=True
        )
        assert isinstance(df, pd.DataFrame)
        assert "metadata" not in df.columns
        assert [0] == df.loc["securityCategories"][0]
        assert "metadata-value" == df.loc["metadata-key"][0]
