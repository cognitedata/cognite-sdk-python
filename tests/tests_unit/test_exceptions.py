from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from pytest_httpx2 import HTTPXMock

from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError, CogniteProjectAccessError
from tests.utils import get_url

if TYPE_CHECKING:
    from pytest_httpx2 import HTTPXMock

    from cognite.client import AsyncCogniteClient, CogniteClient


@pytest.fixture
def mock_get_400_error(httpx2_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient) -> None:
    httpx2_mock.add_response(
        method="GET",
        url=get_url(async_client.assets, "/any"),
        status_code=400,
        json={"error": {"message": "bla", "extra": {"haha": "blabla"}, "other": "yup"}},
    )


@pytest.mark.allow_no_semaphore(
    "These tests use the top-level cognite_client.get(...) nullcontext path to verify "
    "error parsing and cluster info; concurrency limiting is unrelated."
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

    def test_api_error_with_non_string_message(
        self, httpx2_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
    ) -> None:
        validation_errors = [{"type": "value_error", "loc": ["body", "tools", 0], "msg": "Invalid schema"}]
        httpx2_mock.add_response(
            method="GET",
            url=get_url(async_client.assets, "/any"),
            status_code=400,
            json={"error": {"message": validation_errors, "code": 400}},
        )
        with pytest.raises(CogniteAPIError) as exc_info:
            cognite_client.get(url=f"/api/v1/projects/{cognite_client.config.project}/any")

        err = exc_info.value
        assert err.message == validation_errors
        assert '"msg":"Invalid schema"' in str(err)
        assert '"loc":["body","tools",0]' in str(err)
        assert "| code: 400" in str(err)


@pytest.mark.parametrize("req_id", ["abc", None])
@pytest.mark.parametrize(
    "exc, kw",
    (
        (CogniteAPIError, {"message": "bla", "code": 400}),
        (CogniteProjectAccessError, {"project": "proj", "maybe_projects": None}),
    ),
)
def test_timestamp_is_in_exception_str(
    exc: type[CogniteAPIError | CogniteProjectAccessError],
    kw: dict[str, str | int | None],
    req_id: str | None,
) -> None:
    exc_instance = exc(**kw, x_request_id=req_id)  # type: ignore [arg-type]
    for s in repr(exc_instance), str(exc_instance):
        assert f"X-Request-ID: {req_id or 'None'} | time: 2" in s  # todo: year 3000+ :wink:
        assert s.endswith("Z")
