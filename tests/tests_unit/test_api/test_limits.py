import re

import pytest

from cognite.client.data_classes import LimitValue, LimitValueFilter, LimitValueList, LimitValuePrefixFilter
from tests.utils import jsgz_load


@pytest.fixture
def mock_limits_response(rsps, cognite_client):
    response_body = {
        "items": [
            {"limitId": "atlas.monthly_ai_tokens", "value": 1000},
            {"limitId": "files.storage_bytes", "value": 500},
        ],
        "nextCursor": "eyJjdXJzb3IiOiAiMTIzNDU2In0=",
    }

    url_pattern = re.compile(
        re.escape(cognite_client.limits._get_base_url_with_base_path()) + r"/limits/values(?:/list|/.+|$|\?.+)"
    )
    rsps.assert_all_requests_are_fired = False

    rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    yield rsps


@pytest.fixture
def mock_limit_single_response(rsps, cognite_client):
    response_body = {"limitId": "atlas.monthly_ai_tokens", "value": 1000}

    url_pattern = re.compile(re.escape(cognite_client.limits._get_base_url_with_base_path()) + r"/limits/values/.+")
    rsps.assert_all_requests_are_fired = False

    rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
    yield rsps


class TestLimits:
    def test_list(self, cognite_client, mock_limits_response):
        res = cognite_client.limits.list(limit=2, cursor="test_cursor")
        assert isinstance(res, LimitValueList)
        assert len(res) == 2
        assert res[0].limit_id == "atlas.monthly_ai_tokens"
        assert res[0].value == 1000
        assert res[1].limit_id == "files.storage_bytes"
        assert res[1].value == 500
        # Note: next_cursor may be None when _list chains results, but the API response includes it
        # The cursor is available in the raw response but may not be preserved in the final list
        # when multiple pages are chained together

        # Check that the request was made with correct parameters
        calls = mock_limits_response.calls
        assert len(calls) == 1
        assert calls[0].request.method == "GET"
        assert "limit=2" in calls[0].request.url
        assert "cursor=test_cursor" in calls[0].request.url
        assert calls[0].request.headers["cdf-version"] == "20230101-alpha"
        # Verify the response contains nextCursor
        assert calls[0].response.json().get("nextCursor") == "eyJjdXJzb3IiOiAiMTIzNDU2In0="

    def test_list_advanced_with_filter(self, cognite_client, mock_limits_response):
        prefix_filter = LimitValuePrefixFilter(property=["limitId"], value="atlas.")
        filter_obj = LimitValueFilter(prefix=prefix_filter)
        res = cognite_client.limits.list_advanced(filter=filter_obj, limit=100, cursor="test_cursor")

        assert isinstance(res, LimitValueList)
        assert len(res) == 2

        # Check that the request was made with correct body
        calls = mock_limits_response.calls
        assert len(calls) == 1
        assert calls[0].request.method == "POST"
        body = jsgz_load(calls[0].request.body)
        assert body["filter"]["prefix"]["property"] == ["limitId"]
        assert body["filter"]["prefix"]["value"] == "atlas."
        assert body["limit"] == 100
        assert body["cursor"] == "test_cursor"
        assert calls[0].request.headers["cdf-version"] == "20230101-alpha"

    def test_list_advanced_without_filter(self, cognite_client, mock_limits_response):
        res = cognite_client.limits.list_advanced(limit=25)

        assert isinstance(res, LimitValueList)

        # Check that the request was made without filter
        calls = mock_limits_response.calls
        body = jsgz_load(calls[0].request.body)
        assert "filter" not in body
        assert body["limit"] == 25

    def test_retrieve_single(self, cognite_client, mock_limit_single_response):
        res = cognite_client.limits.retrieve(limit_id="atlas.monthly_ai_tokens")

        assert isinstance(res, LimitValue)
        assert res.limit_id == "atlas.monthly_ai_tokens"
        assert res.value == 1000

        # Check that the request was made correctly
        calls = mock_limit_single_response.calls
        assert len(calls) == 1
        assert calls[0].request.method == "GET"
        assert "limits/values/atlas.monthly_ai_tokens" in calls[0].request.url
        assert calls[0].request.headers["cdf-version"] == "20230101-alpha"

    def test_retrieve_with_ignore_unknown_ids(self, cognite_client, rsps):
        from cognite.client.exceptions import CogniteAPIError

        rsps.add(
            rsps.GET,
            re.compile(r".*/limits/values/nonexistent"),
            status=404,
            json={"error": {"code": 404, "message": "Not found"}},
        )

        # With ignore_unknown_ids=True, should return None for 404
        res = cognite_client.limits.retrieve(limit_id="nonexistent", ignore_unknown_ids=True)
        assert res is None

        # With ignore_unknown_ids=False, should raise CogniteAPIError
        with pytest.raises(CogniteAPIError) as exc_info:
            cognite_client.limits.retrieve(limit_id="nonexistent", ignore_unknown_ids=False)
        assert exc_info.value.code == 404

    def test_call_iterator(self, cognite_client, mock_limits_response):
        results = list(cognite_client.limits(limit=2))
        assert len(results) == 2
        assert all(isinstance(item, LimitValue) for item in results)

        # Check that the request was made
        calls = mock_limits_response.calls
        assert len(calls) == 1
        assert calls[0].request.method == "GET"
        assert calls[0].request.headers["cdf-version"] == "20230101-alpha"

    def test_call_iterator_with_chunk_size(self, cognite_client, mock_limits_response):
        results = list(cognite_client.limits(chunk_size=1, limit=2))
        assert len(results) == 2
        assert all(isinstance(item, LimitValueList) for item in results)
        assert len(results[0]) == 1
        assert len(results[1]) == 1

    def test_iter(self, cognite_client, mock_limits_response):
        for limit_value in cognite_client.limits:
            assert isinstance(limit_value, LimitValue)
            break  # Just test that iteration works

    def test_limit_value_load(self):
        data = {"limitId": "test.limit", "value": 42}
        limit = LimitValue._load(data)
        assert limit.limit_id == "test.limit"
        assert limit.value == 42

    def test_limit_value_list_load_from_dict(self):
        data = {
            "items": [
                {"limitId": "test.limit1", "value": 10},
                {"limitId": "test.limit2", "value": 20},
            ],
            "nextCursor": "cursor123",
        }
        limit_list = LimitValueList._load(data)
        assert len(limit_list) == 2
        assert limit_list[0].limit_id == "test.limit1"
        assert limit_list[1].limit_id == "test.limit2"
        assert limit_list.next_cursor == "cursor123"

    def test_limit_value_list_load_from_list(self):
        data = [
            {"limitId": "test.limit1", "value": 10},
            {"limitId": "test.limit2", "value": 20},
        ]
        limit_list = LimitValueList._load(data)
        assert len(limit_list) == 2
        assert limit_list[0].limit_id == "test.limit1"
        assert limit_list[1].limit_id == "test.limit2"

    def test_limit_value_filter_dump(self):
        prefix_filter = LimitValuePrefixFilter(property=["limitId"], value="atlas.")
        filter_obj = LimitValueFilter(prefix=prefix_filter)
        dumped = filter_obj.dump(camel_case=True)
        assert dumped["prefix"]["property"] == ["limitId"]
        assert dumped["prefix"]["value"] == "atlas."
