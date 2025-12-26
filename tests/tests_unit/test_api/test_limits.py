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
    response_body = {
        "items": [
            {"limitId": "atlas.monthly_ai_tokens", "value": 1000},
            {"limitId": "files.storage_bytes", "value": 500},
        ],
        "nextCursor": "eyJjdXJzb3IiOiAiMTIzNDU2In0=",
    }

    base_url = get_url(async_client.limits)
    get_url_pattern = re.compile(re.escape(base_url) + r"/limits/values(?:\?.+|$)")
    post_url_pattern = re.compile(re.escape(base_url) + r"/limits/values/list")

    httpx_mock.add_response(method="GET", url=get_url_pattern, status_code=200, json=response_body, is_optional=True)
    httpx_mock.add_response(method="POST", url=post_url_pattern, status_code=200, json=response_body, is_optional=True)
    httpx_mock.add_response(
        method="POST", url=post_url_pattern, status_code=200, json={"items": response_body["items"]}, is_optional=True
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
        expected_items = [
            {"limitId": "atlas.monthly_ai_tokens", "value": 1000},
            {"limitId": "files.storage_bytes", "value": 500},
        ]
        assert expected_items == res.dump(camel_case=True)

        requests = httpx_mock.get_requests()
        assert requests[0].method == "GET"
        assert "limit=2" in str(requests[0].url) and "cursor=test_cursor" in str(requests[0].url)
        assert requests[0].headers["cdf-version"] == "20230101-alpha"

    def test_list_advanced(
        self, cognite_client: CogniteClient, mock_limits_response: HTTPXMock, httpx_mock: HTTPXMock
    ) -> None:
        prefix_filter = LimitValuePrefixFilter(property=["limitId"], value="atlas.")
        filter_obj = LimitValueFilter(prefix=prefix_filter)
        res = cognite_client.limits.list_advanced(filter=filter_obj, limit=2, cursor="test_cursor")

        assert len(res) == 2
        expected_body = {
            "filter": {"prefix": {"property": ["limitId"], "value": "atlas."}},
            "limit": 2,
            "cursor": "test_cursor",
        }
        assert expected_body == jsgz_load(httpx_mock.get_requests()[0].content)
        assert httpx_mock.get_requests()[0].headers["cdf-version"] == "20230101-alpha"

    def test_list_advanced_unlimited(
        self, cognite_client: CogniteClient, httpx_mock: HTTPXMock, async_client: AsyncCogniteClient
    ) -> None:
        response_body = {
            "items": [{"limitId": "atlas.monthly_ai_tokens", "value": 1000}],
            "nextCursor": "cursor",
        }
        base_url = get_url(async_client.limits)
        post_url_pattern = re.compile(re.escape(base_url) + r"/limits/values/list")

        httpx_mock.add_response(
            method="POST", url=post_url_pattern, status_code=200, json=response_body, is_optional=True
        )
        httpx_mock.add_response(
            method="POST", url=post_url_pattern, status_code=200, json={"items": []}, is_optional=True
        )

        res = cognite_client.limits.list_advanced(limit=-1)
        assert isinstance(res, LimitValueList)

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
        assert httpx_mock.get_requests()[0].headers["cdf-version"] == "20230101-alpha"

    def test_call_iterator_with_chunk_size(
        self, cognite_client: CogniteClient, mock_limits_response: HTTPXMock, httpx_mock: HTTPXMock
    ) -> None:
        results = list(cognite_client.limits(chunk_size=1, limit=2))
        assert len(results) == 2
        assert all(isinstance(item, LimitValueList) for item in results)
