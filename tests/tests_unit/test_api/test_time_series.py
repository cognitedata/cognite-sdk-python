import re

import pytest

from cognite.client import CogniteClient
from cognite.client.api.time_series import TimeSeries, TimeSeriesFilter, TimeSeriesList, TimeSeriesUpdate
from tests.utils import jsgz_load

TS_API = CogniteClient().time_series


@pytest.fixture
def mock_ts_response(rsps):
    response_body = {
        "data": {
            "items": [
                {
                    "id": 0,
                    "externalId": "string",
                    "name": "string",
                    "isString": True,
                    "metadata": {},
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

    url_pattern = re.compile(re.escape(TS_API._base_url) + "/.+")
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
        res = TS_API.update(TimeSeriesUpdate(id=1).description_set("blabla"))
        assert isinstance(res, TimeSeries)
        assert mock_ts_response.calls[0].response.json()["data"]["items"][0] == res.dump(camel_case=True)

    def test_update_multiple(self, mock_ts_response):
        res = TS_API.update([TimeSeriesUpdate(id=1).description_set("blabla")])
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
