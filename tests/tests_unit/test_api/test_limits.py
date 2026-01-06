import re

import pytest
from pytest_httpx import HTTPXMock

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.data_classes import Limit, LimitList
from tests.utils import get_url

ATLAS_LIMIT: dict[str, str | int] = {"limitId": "atlas.monthly_ai_tokens", "value": 1000}
NONEXISTENT_ID = "nonexistent.limit.id"


@pytest.fixture
def limits_url(async_client: AsyncCogniteClient) -> str:
    return get_url(async_client.limits) + "/limits/values"


class TestLimits:
    def test_retrieve_single(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        async_client: AsyncCogniteClient,
        limits_url: str,
    ) -> None:
        httpx_mock.add_response(
            method="GET",
            url=f"{limits_url}/{ATLAS_LIMIT['limitId']}",
            status_code=200,
            json=ATLAS_LIMIT,
        )

        limit_id = str(ATLAS_LIMIT["limitId"])
        res = cognite_client.limits.retrieve(id=limit_id)

        assert isinstance(res, Limit)
        assert ATLAS_LIMIT == res.dump(camel_case=True)

        self._verify_request(httpx_mock, async_client, limit_id)

    def test_retrieve_single_not_found(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        async_client: AsyncCogniteClient,
        limits_url: str,
    ) -> None:
        httpx_mock.add_response(
            method="GET",
            url=f"{limits_url}/{NONEXISTENT_ID}",
            status_code=404,
            json={"error": {"message": "Not Found"}},
        )

        res = cognite_client.limits.retrieve(id=NONEXISTENT_ID)

        assert res is None

    def test_list(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        async_client: AsyncCogniteClient,
        limits_url: str,
    ) -> None:
        limits_data = [
            {"limitId": "atlas.monthly_ai_tokens", "value": 1000},
            {"limitId": "files.storage_bytes", "value": 5000},
        ]
        url_pattern = re.compile(re.escape(limits_url) + r"(?:\?.+)?")
        httpx_mock.add_response(
            method="GET",
            url=url_pattern,
            status_code=200,
            json={"items": limits_data},
        )

        res = cognite_client.limits.list()

        assert isinstance(res, LimitList)
        assert len(res) == 2
        assert all(isinstance(limit, Limit) for limit in res)

    @staticmethod
    def _verify_request(httpx_mock: HTTPXMock, async_client: AsyncCogniteClient, expected_id: str) -> None:
        requests = httpx_mock.get_requests()
        assert len(requests) == 1

        request = requests[0]
        assert request.method == "GET"
        assert f"limits/values/{expected_id}" in str(request.url)
