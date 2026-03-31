from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from pytest_httpx import HTTPXMock

from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError
from tests.utils import get_url

if TYPE_CHECKING:
    from pytest_httpx import HTTPXMock

    from cognite.client import AsyncCogniteClient, CogniteClient


@pytest.fixture
def mock_get_400_error(httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient) -> None:
    httpx_mock.add_response(
        method="GET",
        url=get_url(async_client.assets, "/any"),
        status_code=400,
        json={"error": {"message": "bla", "extra": {"haha": "blabla"}, "other": "yup"}},
    )


class TestAPIError:
    def test_api_error(self) -> None:
        e = CogniteAPIError(
            message="bla",
            code=200,
            x_request_id="abc",
            missing=[{"id": 123}],
            duplicated=[{"externalId": "abc"}],
            successful=["bla"],
        )
        assert "bla" == e.message
        assert 200 == e.code
        assert "abc" == e.x_request_id
        assert [{"id": 123}] == e.missing
        assert [{"externalId": "abc"}] == e.duplicated

        assert "bla" in str(e)

    def test_unknown_fields_in_api_error(self, mock_get_400_error: None, cognite_client: CogniteClient) -> None:
        try:
            cognite_client.get(url=f"/api/v1/projects/{cognite_client.config.project}/any")
            assert False, "Call did not raise exception"
        except CogniteAPIError as e:
            assert {"extra": {"haha": "blabla"}, "other": "yup"} == e.extra

    def test_api_errors_contain_cluster_info(self, mock_get_400_error: None, cognite_client: CogniteClient) -> None:
        try:
            cognite_client.get(url=f"/api/v1/projects/{cognite_client.config.project}/any")
            assert False, "Call did not raise exception"
        except CogniteAPIError as e:
            assert e.cluster == "api"
