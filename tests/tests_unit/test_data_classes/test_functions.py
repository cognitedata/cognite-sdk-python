from __future__ import annotations

import datetime
import re
from collections.abc import Iterator
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock

import pytest
from pytest_httpx import HTTPXMock

from cognite.client import CogniteClient
from cognite.client.data_classes import Function, FunctionCallLog, FunctionCallLogEntry
from cognite.client.utils._time import datetime_to_ms
from tests.utils import get_url, jsgz_load

if TYPE_CHECKING:
    from pytest_httpx import HTTPXMock

    from cognite.client import AsyncCogniteClient, CogniteClient


@pytest.fixture
def empty_function(async_client: AsyncCogniteClient) -> Function:
    return Function(
        id=123,
        created_time=123,
        name="bla",
        external_id=None,
        description=None,
        owner=None,
        status="bla",
        file_id=123,
        function_path="bla",
        secrets=None,
        env_vars=None,
        cpu=None,
        memory=None,
        runtime=None,
        runtime_version=None,
        metadata=None,
        error=None,
        last_called=None,
    ).set_client_ref(async_client)


@pytest.fixture
def function(async_client: AsyncCogniteClient) -> Function:
    return Function(
        id=123,
        name="my-function",
        external_id="my-function",
        description="some description",
        owner="somebody",
        status="Deploying",
        file_id=456,
        function_path="handler.py",
        created_time=123,
        secrets={},
        env_vars=None,
        metadata=None,
        cpu=None,
        memory=None,
        runtime=None,
        runtime_version=None,
        error=None,
        last_called=123456789,
    ).set_client_ref(async_client)


@pytest.fixture
def mock_function_call_resp(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> Iterator[HTTPXMock]:
    response_body = {
        "items": [
            {
                "endTime": 1647594536056,
                "functionId": 2586071956285058,
                "id": 395335920687780,
                "startTime": 1647593036056,
                "status": "Timeout",
            }
        ]
    }
    url_pattern = re.compile(re.escape(get_url(async_client.functions)) + "/.+")
    httpx_mock.add_response(method="POST", url=url_pattern, status_code=200, json=response_body)
    yield httpx_mock


class TestFunction:
    def test_update(self, empty_function: Function, function: Function) -> None:
        empty_function._cognite_client.functions.retrieve = AsyncMock(return_value=function)  # type: ignore[method-assign]

        empty_function.update()
        assert function == empty_function

    def test_update_on_deleted_function(self, empty_function: Function) -> None:
        empty_function._cognite_client.functions.retrieve = AsyncMock(return_value=None)  # type: ignore[method-assign]
        empty_function.update()


class TestFunctionCall:
    def test_get_function_call_no_filter(
        self, cognite_client: CogniteClient, mock_function_call_resp: HTTPXMock
    ) -> None:
        cognite_client.functions.calls.list(function_id=2586071956285058)
        calls = mock_function_call_resp.get_requests()
        assert 1 == len(calls)
        assert {
            "limit": 25,
        } == jsgz_load(calls[0].content)

    def test_get_function_call_with_filter(
        self, cognite_client: CogniteClient, mock_function_call_resp: HTTPXMock
    ) -> None:
        cognite_client.functions.calls.list(
            function_id=2586071956285058, status="Completed", schedule_id=395335920687780
        )
        calls = mock_function_call_resp.get_requests()
        assert 1 == len(calls)
        assert {
            "limit": 25,
            "filter": {
                "scheduleId": 395335920687780,
                "status": "Completed",
            },
        } == jsgz_load(calls[0].content)


class TestFunctionCallLog:
    @pytest.fixture(scope="class")
    def entries(self) -> list[tuple[datetime.datetime, str]]:
        start_ts = datetime.datetime(2023, 10, 4, 10, 30, 4, 123000, tzinfo=datetime.timezone.utc)
        ms_delta = datetime.timedelta(milliseconds=100)
        return [(start_ts + i * ms_delta, f"line {i}") for i in range(10)]

    def test_to_text_without_timestamps(self, entries: list[tuple[datetime.datetime, str]]) -> None:
        log = FunctionCallLog(
            resources=[FunctionCallLogEntry(timestamp=datetime_to_ms(ts), message=msg) for (ts, msg) in entries]
        )
        expected = "\n".join([msg for (_, msg) in entries])
        assert log.to_text() == expected

    def test_to_text_with_timestamps(self, entries: list[tuple[datetime.datetime, str]]) -> None:
        log = FunctionCallLog(
            resources=[FunctionCallLogEntry(timestamp=datetime_to_ms(ts), message=msg) for (ts, msg) in entries]
        )
        expected = "\n".join(entry._format(with_timestamps=True) for entry in log)
        assert log.to_text(with_timestamps=True) == expected


class TestFunctionCallLogEntry:
    def test_format_with_timestamp(self) -> None:
        ts = datetime.datetime(2023, 10, 4, 10, 30, 4, 123000, tzinfo=datetime.timezone.utc)
        entry = FunctionCallLogEntry(timestamp=datetime_to_ms(ts), message="line one")

        assert entry._format(with_timestamps=True) == f"[{ts}] line one"

    def test_format_without_timestamp(self) -> None:
        ts = datetime.datetime(2023, 10, 4, 10, 30, 4, 123)
        entry = FunctionCallLogEntry(timestamp=datetime_to_ms(ts), message="line one")

        assert entry._format(with_timestamps=False) == "line one"

    def test_format_with_none_timestamp(self) -> None:
        entry = FunctionCallLogEntry(timestamp=None, message="line one")

        assert entry._format(with_timestamps=True) == "line one"
        assert entry._format(with_timestamps=False) == "line one"
