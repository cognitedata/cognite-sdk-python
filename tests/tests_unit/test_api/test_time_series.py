import re
from unittest import mock

import pytest

from cognite.client import CogniteClient
from cognite.client._api.time_series import TimeSeries, TimeSeriesFilter, TimeSeriesList, TimeSeriesUpdate, utils
from tests.utils import jsgz_load

COGNITE_CLIENT = CogniteClient(debug=True)
TS_API = COGNITE_CLIENT.time_series


@pytest.fixture
def mock_ts_response(rsps):
    response_body = {
        "data": {
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
    }
    url_pattern = re.compile(re.escape(TS_API._base_url) + "/timeseries(?:/byids|/update|/delete|/search|$|\?.+)")
    rsps.assert_all_requests_are_fired = False

    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
    yield rsps


class TestTimeSeries:
    def test_get_single(self, mock_ts_response):
        res = TS_API.get(id=1)
        assert isinstance(res, TimeSeries)
        assert mock_ts_response.calls[0].response.json()["data"]["items"][0] == res.dump(camel_case=True)

    def test_get_multiple(self, mock_ts_response):
        res = TS_API.get(id=[1])
        assert isinstance(res, TimeSeriesList)
        assert mock_ts_response.calls[0].response.json()["data"]["items"] == res.dump(camel_case=True)

    def test_list(self, mock_ts_response):
        res = TS_API.list()
        assert mock_ts_response.calls[0].response.json()["data"]["items"] == res.dump(camel_case=True)

    def test_create_single(self, mock_ts_response):
        res = TS_API.create(TimeSeries(external_id="1", name="blabla"))
        assert isinstance(res, TimeSeries)
        assert mock_ts_response.calls[0].response.json()["data"]["items"][0] == res.dump(camel_case=True)

    def test_create_multiple(self, mock_ts_response):
        res = TS_API.create([TimeSeries(external_id="1", name="blabla")])
        assert isinstance(res, TimeSeriesList)
        assert mock_ts_response.calls[0].response.json()["data"]["items"] == res.dump(camel_case=True)

    def test_iter_single(self, mock_ts_response):
        for asset in TS_API:
            assert mock_ts_response.calls[0].response.json()["data"]["items"][0] == asset.dump(camel_case=True)

    def test_iter_chunk(self, mock_ts_response):
        for assets in TS_API(chunk_size=1):
            assert mock_ts_response.calls[0].response.json()["data"]["items"] == assets.dump(camel_case=True)

    def test_delete_single(self, mock_ts_response):
        res = TS_API.delete(id=1)
        assert {"items": [{"id": 1}]} == jsgz_load(mock_ts_response.calls[0].request.body)
        assert res is None

    def test_delete_multiple(self, mock_ts_response):
        res = TS_API.delete(id=[1])
        assert {"items": [{"id": 1}]} == jsgz_load(mock_ts_response.calls[0].request.body)
        assert res is None

    def test_update_with_resource_class(self, mock_ts_response):
        res = TS_API.update(TimeSeries(id=1))
        assert isinstance(res, TimeSeries)
        assert mock_ts_response.calls[0].response.json()["data"]["items"][0] == res.dump(camel_case=True)

    def test_update_with_update_class(self, mock_ts_response):
        res = TS_API.update(TimeSeriesUpdate(id=1).description.set("blabla"))
        assert isinstance(res, TimeSeries)
        assert mock_ts_response.calls[0].response.json()["data"]["items"][0] == res.dump(camel_case=True)

    def test_update_multiple(self, mock_ts_response):
        res = TS_API.update([TimeSeriesUpdate(id=1).description.set("blabla")])
        assert isinstance(res, TimeSeriesList)
        assert mock_ts_response.calls[0].response.json()["data"]["items"] == res.dump(camel_case=True)

    def test_search(self, mock_ts_response):
        res = TS_API.search()
        assert mock_ts_response.calls[0].response.json()["data"]["items"] == res.dump(camel_case=True)

    def test_search_with_filter(self, mock_ts_response):
        res = TS_API.search(name="n", description="d", query="q", filter=TimeSeriesFilter(unit="bla"))
        assert mock_ts_response.calls[0].response.json()["data"]["items"] == res.dump(camel_case=True)
        req_body = jsgz_load(mock_ts_response.calls[0].request.body)
        assert "bla" == req_body["filter"]["unit"]
        assert {"name": "n", "description": "d", "query": "q"} == req_body["search"]

    def test_events_update_object(self):
        assert isinstance(
            TimeSeriesUpdate(1)
            .asset_id.set(1)
            .asset_id.set(None)
            .description.set("")
            .description.set(None)
            .external_id.set("1")
            .external_id.set(None)
            .metadata.set({})
            .metadata.add({})
            .metadata.remove([])
            .name.set("")
            .name.set(None)
            .security_categories.set([])
            .security_categories.add([])
            .security_categories.remove([])
            .unit.set("")
            .unit.set(None),
            TimeSeriesUpdate,
        )


@pytest.mark.dsl
class TestPlotTimeSeries:
    @pytest.fixture
    def mock_get_dps(self, rsps):
        rsps.add(
            rsps.POST,
            TS_API._base_url + "/timeseries/data/get",
            status=200,
            json={
                "data": {
                    "items": [
                        {
                            "id": 0,
                            "externalId": "string1",
                            "datapoints": [{"timestamp": utils.timestamp_to_ms("1d-ago"), "average": 1}],
                        }
                    ]
                }
            },
        )
        rsps.add(
            rsps.POST,
            TS_API._base_url + "/timeseries/data/get",
            status=200,
            json={
                "data": {
                    "items": [
                        {
                            "id": 0,
                            "externalId": "string1",
                            "datapoints": [{"timestamp": i * 10000, "average": i} for i in range(5000)],
                        }
                    ]
                }
            },
        )

    @mock.patch("matplotlib.pyplot.show")
    @mock.patch("pandas.core.frame.DataFrame.rename")
    def test_plot_time_series_name_labels(self, pandas_rename_mock, plt_show_mock, mock_ts_response, mock_get_dps):
        res = TS_API.get(id=0)
        df_mock = mock.MagicMock()
        pandas_rename_mock.return_value = df_mock
        res.plot(aggregates=["average"], granularity="1d")

        assert {0: "stringname", "0|average": "stringname|average"} == pandas_rename_mock.call_args[1]["columns"]
        assert 1 == df_mock.plot.call_count
        assert 1 == plt_show_mock.call_count

    @mock.patch("matplotlib.pyplot.show")
    @mock.patch("pandas.core.frame.DataFrame.plot")
    def test_plot_time_series_id_labels(self, pandas_plot_mock, plt_show_mock, mock_ts_response, mock_get_dps):
        res = TS_API.get(id=0)
        res.plot(id_labels=True)

        assert 1 == pandas_plot_mock.call_count
        assert 1 == plt_show_mock.call_count

    @mock.patch("matplotlib.pyplot.show")
    @mock.patch("pandas.core.frame.DataFrame.rename")
    def test_plot_time_series_list_name_labels(self, pandas_rename_mock, plt_show_mock, mock_ts_response, mock_get_dps):
        res = TS_API.get(id=[0])
        df_mock = mock.MagicMock()
        pandas_rename_mock.return_value = df_mock
        res.plot(aggregates=["average"], granularity="1h")
        assert {0: "stringname", "0|average": "stringname|average"} == pandas_rename_mock.call_args[1]["columns"]
        assert 1 == df_mock.plot.call_count
        assert 1 == plt_show_mock.call_count

    @mock.patch("matplotlib.pyplot.show")
    @mock.patch("pandas.core.frame.DataFrame.plot")
    def test_plot_time_series_list_id_labels(self, pandas_plot_mock, plt_show_mock, mock_ts_response, mock_get_dps):
        res = TS_API.get(id=[0])
        res.plot(id_labels=True)

        assert 1 == pandas_plot_mock.call_count
        assert 1 == plt_show_mock.call_count


@pytest.fixture
def mock_time_series_empty(rsps):
    url_pattern = re.compile(re.escape(TS_API._base_url) + "/.+")
    rsps.assert_all_requests_are_fired = False
    rsps.add(rsps.POST, url_pattern, status=200, json={"data": {"items": []}})
    rsps.add(rsps.GET, url_pattern, status=200, json={"data": {"items": []}})
    yield rsps


@pytest.mark.dsl
class TestPandasIntegration:
    def test_time_series_list_to_pandas(self, mock_ts_response):
        import pandas as pd

        df = TS_API.list().to_pandas()
        assert isinstance(df, pd.DataFrame)
        assert 1 == df.shape[0]
        assert {"metadata-key": "metadata-value"} == df["metadata"][0]

    def test_time_series_list_to_pandas_empty(self, mock_time_series_empty):
        import pandas as pd

        df = TS_API.list().to_pandas()
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_time_series_to_pandas(self, mock_ts_response):
        import pandas as pd

        df = TS_API.get(id=1).to_pandas()
        assert isinstance(df, pd.DataFrame)
        assert "metadata" not in df.columns
        assert [0] == df.loc["securityCategories"][0]
        assert "metadata-value" == df.loc["metadata-key"][0]
