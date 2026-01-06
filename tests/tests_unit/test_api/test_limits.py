from typing import cast

import pytest
from pytest_httpx import HTTPXMock

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.data_classes import Limit, LimitList
from tests.utils import get_url, jsgz_load

# Test data constants
ATLAS_LIMIT = {"limitId": "atlas.monthly_ai_tokens", "value": 1000}
FILES_LIMIT = {"limitId": "files.storage_bytes", "value": 5000}
NONEXISTENT_ID = "nonexistent.limit.id"
ANOTHER_NONEXISTENT_ID = "another.nonexistent"


@pytest.fixture
def limits_list_url(async_client: AsyncCogniteClient) -> str:
    return get_url(async_client.limits) + "/limits/values/list"


@pytest.fixture
def mock_limit_response(httpx_mock: HTTPXMock, limits_list_url: str) -> HTTPXMock:
    httpx_mock.add_response(
        method="POST",
        url=limits_list_url,
        status_code=200,
        json={"items": [ATLAS_LIMIT]},
    )
    return httpx_mock


class TestLimits:
    def test_retrieve_multiple(
        self,
        cognite_client: CogniteClient,
        mock_limit_response: HTTPXMock,
        httpx_mock: HTTPXMock,
        async_client: AsyncCogniteClient,
        limits_list_url: str,
    ) -> None:
        limit_id = cast(str, ATLAS_LIMIT["limitId"])
        res = cognite_client.limits.retrieve_multiple(ids=[limit_id])

        assert isinstance(res, LimitList)
        assert len(res) == 1
        assert isinstance(res[0], Limit)
        assert ATLAS_LIMIT == res[0].dump(camel_case=True)

        self._verify_request(httpx_mock, async_client, [limit_id])

    def test_retrieve_multiple_non_existing(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        limits_list_url: str,
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=limits_list_url,
            status_code=200,
            json={"items": []},
        )

        res = cognite_client.limits.retrieve_multiple(ids=["nonexistent"])

        assert isinstance(res, LimitList)
        assert len(res) == 0

    def test_retrieve_multiple_by_ids(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        async_client: AsyncCogniteClient,
        limits_list_url: str,
    ) -> None:
        atlas_id = cast(str, ATLAS_LIMIT["limitId"])
        files_id = cast(str, FILES_LIMIT["limitId"])
        existing_ids = [atlas_id, files_id]
        non_existing_ids = [NONEXISTENT_ID, ANOTHER_NONEXISTENT_ID]
        all_ids = existing_ids + non_existing_ids

        httpx_mock.add_response(
            method="POST",
            url=limits_list_url,
            status_code=200,
            json={"items": [ATLAS_LIMIT, FILES_LIMIT]},
        )

        res = cognite_client.limits.retrieve_multiple(ids=all_ids)

        assert isinstance(res, LimitList)
        assert len(res) == 2
        assert all(isinstance(limit, Limit) for limit in res)

        limit_ids = {limit.limit_id for limit in res}
        assert limit_ids == set(existing_ids)

        limit_by_id = {limit.limit_id: limit.value for limit in res}
        assert limit_by_id[atlas_id] == ATLAS_LIMIT["value"]
        assert limit_by_id[files_id] == FILES_LIMIT["value"]

        self._verify_request(httpx_mock, async_client, all_ids)

    @staticmethod
    def _verify_request(httpx_mock: HTTPXMock, async_client: AsyncCogniteClient, expected_ids: list[str]) -> None:
        requests = httpx_mock.get_requests()
        assert len(requests) == 1

        request = requests[0]
        assert request.method == "POST"
        assert "limits/values/list" in str(request.url)
        assert request.headers["cdf-version"] == f"{async_client.config.api_subversion}-alpha"

        request_body = jsgz_load(request.content)
        assert "advancedFilter" in request_body
        assert request_body["advancedFilter"]["in"]["property"] == ["limitId"]
        assert set(request_body["advancedFilter"]["in"]["values"]) == set(expected_ids)
