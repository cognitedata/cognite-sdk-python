import re

import pytest
from pytest_httpx import HTTPXMock

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.data_classes import LimitValue
from tests.utils import get_url


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
