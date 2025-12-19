import re

import pytest
from pytest_httpx import HTTPXMock

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.data_classes import LimitValue, LimitValueFilter, LimitValueList, LimitValuePrefixFilter
from tests.utils import get_url, jsgz_load


@pytest.fixture
def mock_limits_response(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> HTTPXMock:
    response_body_with_cursor = {
        "items": [
            {"limitId": "atlas.monthly_ai_tokens", "value": 1000},
            {"limitId": "files.storage_bytes", "value": 500},
        ],
        "nextCursor": "eyJjdXJzb3IiOiAiMTIzNDU2In0=",
    }
    response_body_no_cursor = {
        "items": [
            {"limitId": "atlas.monthly_ai_tokens", "value": 1000},
            {"limitId": "files.storage_bytes", "value": 500},
        ],
    }

    base_url = get_url(async_client.limits)
    get_url_pattern = re.compile(re.escape(base_url) + r"/limits/values(?:\?.+|$)")
    post_url_pattern = re.compile(re.escape(base_url) + r"/limits/values/list")

    httpx_mock.add_response(
        method="GET", url=get_url_pattern, status_code=200, json=response_body_with_cursor, is_optional=True
    )
    # For POST requests, return cursor on first call, then no cursor to stop pagination
    httpx_mock.add_response(
        method="POST", url=post_url_pattern, status_code=200, json=response_body_with_cursor, is_optional=True
    )
    httpx_mock.add_response(
        method="POST", url=post_url_pattern, status_code=200, json=response_body_no_cursor, is_optional=True
    )
    return httpx_mock


@pytest.fixture
def mock_limit_single_response(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> HTTPXMock:
    response_body = {"limitId": "atlas.monthly_ai_tokens", "value": 1000}

    base_url = get_url(async_client.limits)
    url_pattern = re.compile(re.escape(base_url) + r"/limits/values/.+")

    httpx_mock.add_response(method="GET", url=url_pattern, status_code=200, json=response_body)
    return httpx_mock


class TestLimits:
    def test_list(self, cognite_client: CogniteClient, mock_limits_response: HTTPXMock, httpx_mock: HTTPXMock) -> None:
        res = cognite_client.limits.list(limit=2, cursor="test_cursor")
        assert isinstance(res, LimitValueList)
        assert len(res) == 2
        expected_items = [
            {"limitId": "atlas.monthly_ai_tokens", "value": 1000},
            {"limitId": "files.storage_bytes", "value": 500},
        ]
        assert expected_items == res.dump(camel_case=True)

        requests = httpx_mock.get_requests()
        assert len(requests) == 1
        assert requests[0].method == "GET"
        assert "limit=2" in str(requests[0].url)
        assert "cursor=test_cursor" in str(requests[0].url)
        assert requests[0].headers["cdf-version"] == "20230101-alpha"

    def test_list_advanced_with_filter(
        self, cognite_client: CogniteClient, mock_limits_response: HTTPXMock, httpx_mock: HTTPXMock
    ) -> None:
        prefix_filter = LimitValuePrefixFilter(property=["limitId"], value="atlas.")
        filter_obj = LimitValueFilter(prefix=prefix_filter)
        res = cognite_client.limits.list_advanced(filter=filter_obj, limit=2, cursor="test_cursor")

        assert isinstance(res, LimitValueList)
        assert len(res) == 2

        requests = httpx_mock.get_requests()
        assert len(requests) >= 1
        assert requests[0].method == "POST"
        expected_body = {
            "filter": {"prefix": {"property": ["limitId"], "value": "atlas."}},
            "limit": 2,
            "cursor": "test_cursor",
        }
        assert expected_body == jsgz_load(requests[0].content)
        assert requests[0].headers["cdf-version"] == "20230101-alpha"

    def test_list_advanced_without_filter(
        self, cognite_client: CogniteClient, mock_limits_response: HTTPXMock, httpx_mock: HTTPXMock
    ) -> None:
        res = cognite_client.limits.list_advanced(limit=25)

        assert isinstance(res, LimitValueList)

        requests = httpx_mock.get_requests()
        expected_body = {"limit": 25}
        assert expected_body == jsgz_load(requests[0].content)

    def test_list_advanced_with_unlimited_limit(
        self, cognite_client: CogniteClient, httpx_mock: HTTPXMock, async_client: AsyncCogniteClient
    ) -> None:
        """Test that unlimited limits (-1, float('inf')) work correctly with pagination."""
        response_body_with_cursor = {
            "items": [
                {"limitId": "atlas.monthly_ai_tokens", "value": 1000},
                {"limitId": "files.storage_bytes", "value": 500},
            ],
            "nextCursor": "eyJjdXJzb3IiOiAiMTIzNDU2In0=",
        }
        response_body_no_cursor = {
            "items": [
                {"limitId": "atlas.monthly_ai_tokens", "value": 1000},
                {"limitId": "files.storage_bytes", "value": 500},
            ],
        }

        base_url = get_url(async_client.limits)
        post_url_pattern = re.compile(re.escape(base_url) + r"/limits/values/list")

        # Mock pagination responses: 3 calls x 2 responses each (with cursor, then without to stop)
        for _ in range(3):
            httpx_mock.add_response(
                method="POST", url=post_url_pattern, status_code=200, json=response_body_with_cursor, is_optional=True
            )
            httpx_mock.add_response(
                method="POST", url=post_url_pattern, status_code=200, json=response_body_no_cursor, is_optional=True
            )

        res1 = cognite_client.limits.list_advanced(limit=-1)
        assert isinstance(res1, LimitValueList)

        res2 = cognite_client.limits.list_advanced(limit=float("inf"))  # type: ignore[arg-type]
        assert isinstance(res2, LimitValueList)

        res3 = cognite_client.limits.list_advanced(limit=None)
        assert isinstance(res3, LimitValueList)

    def test_retrieve_single(
        self, cognite_client: CogniteClient, mock_limit_single_response: HTTPXMock, httpx_mock: HTTPXMock
    ) -> None:
        res = cognite_client.limits.retrieve(limit_id="atlas.monthly_ai_tokens")

        assert isinstance(res, LimitValue)
        expected_limit = {"limitId": "atlas.monthly_ai_tokens", "value": 1000}
        assert expected_limit == res.dump(camel_case=True)

        requests = httpx_mock.get_requests()
        assert len(requests) == 1
        assert requests[0].method == "GET"
        assert "limits/values/atlas.monthly_ai_tokens" in str(requests[0].url)
        assert requests[0].headers["cdf-version"] == "20230101-alpha"

    def test_retrieve_with_ignore_unknown_ids(
        self, cognite_client: CogniteClient, httpx_mock: HTTPXMock, async_client: AsyncCogniteClient
    ) -> None:
        from cognite.client.exceptions import CogniteAPIError

        base_url = get_url(async_client.limits)
        url_pattern = re.compile(re.escape(base_url) + r"/limits/values/nonexistent")

        # Mock two 404 responses - one for each call
        httpx_mock.add_response(
            method="GET",
            url=url_pattern,
            status_code=404,
            json={"error": {"code": 404, "message": "Not found"}},
        )
        httpx_mock.add_response(
            method="GET",
            url=url_pattern,
            status_code=404,
            json={"error": {"code": 404, "message": "Not found"}},
        )

        res = cognite_client.limits.retrieve(limit_id="nonexistent", ignore_unknown_ids=True)
        assert res is None

        with pytest.raises(CogniteAPIError) as exc_info:
            cognite_client.limits.retrieve(limit_id="nonexistent", ignore_unknown_ids=False)
        assert exc_info.value.code == 404

    def test_call_iterator(
        self, cognite_client: CogniteClient, mock_limits_response: HTTPXMock, httpx_mock: HTTPXMock
    ) -> None:
        results = list(cognite_client.limits(limit=2))
        assert len(results) == 2
        assert all(isinstance(item, LimitValue) for item in results)

        requests = httpx_mock.get_requests()
        assert len(requests) == 1
        assert requests[0].method == "GET"
        assert requests[0].headers["cdf-version"] == "20230101-alpha"

    def test_call_iterator_with_chunk_size(
        self, cognite_client: CogniteClient, mock_limits_response: HTTPXMock, httpx_mock: HTTPXMock
    ) -> None:
        results = list(cognite_client.limits(chunk_size=1, limit=2))
        assert len(results) == 2
        assert all(isinstance(item, LimitValueList) for item in results)
        assert len(results[0]) == 1
        assert len(results[1]) == 1

    def test_limit_value_load(self) -> None:
        data = {"limitId": "test.limit", "value": 42}
        limit = LimitValue._load(data)
        assert limit.limit_id == "test.limit"
        assert limit.value == 42

    def test_limit_value_list_load_from_dict(self) -> None:
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

    def test_limit_value_list_load_from_list(self) -> None:
        data = [
            {"limitId": "test.limit1", "value": 10},
            {"limitId": "test.limit2", "value": 20},
        ]
        limit_list = LimitValueList._load(data)
        assert len(limit_list) == 2
        assert limit_list[0].limit_id == "test.limit1"
        assert limit_list[1].limit_id == "test.limit2"

    def test_limit_value_filter_dump(self) -> None:
        prefix_filter = LimitValuePrefixFilter(property=["limitId"], value="atlas.")
        filter_obj = LimitValueFilter(prefix=prefix_filter)
        expected = {"prefix": {"property": ["limitId"], "value": "atlas."}}
        assert expected == filter_obj.dump(camel_case=True)
