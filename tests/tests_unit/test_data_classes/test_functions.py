import datetime
import re

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import Function, FunctionCallLog, FunctionCallLogEntry
from cognite.client.utils._time import datetime_to_ms
from tests.utils import jsgz_load


@pytest.fixture
def empty_function(cognite_mock_client_placeholder: CogniteClient):
    return Function(id=123, cognite_client=cognite_mock_client_placeholder)


@pytest.fixture
def function(cognite_mock_client_placeholder: CogniteClient):
    return Function(
        id=123,
        name="my-function",
        external_id="my-function",
        description="some description",
        owner="somebody",
        status="Deploying",
        file_id=456,
        function_path="handler.py",
        created_time="2020-06-19 08:49:37",
        secrets={},
        cognite_client=cognite_mock_client_placeholder,
    )


@pytest.fixture
def mock_function_call_resp(rsps, cognite_client):
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
    url_pattern = re.compile(re.escape(cognite_client.functions._get_base_url_with_base_path()) + "/.+")
    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    yield rsps


class TestFunction:
    def test_update(self, empty_function, function):
        empty_function._cognite_client.functions.retrieve.return_value = function

        empty_function.update()
        assert function == empty_function

    def test_update_on_deleted_function(self, empty_function):
        empty_function._cognite_client.functions.retrieve.return_value = None
        empty_function.update()


class TestFunctionCall:
    def test_get_function_call_no_filter(self, cognite_client, mock_function_call_resp):
        cognite_client.functions.calls.list(function_id=2586071956285058)
        calls = mock_function_call_resp.calls
        assert 1 == len(calls)
        assert {
            "cursor": None,
            "limit": 25,
        } == jsgz_load(calls[0].request.body)

    def test_get_function_call_with_filter(self, cognite_client, mock_function_call_resp):
        cognite_client.functions.calls.list(
            function_id=2586071956285058, status="Completed", schedule_id=395335920687780
        )
        calls = mock_function_call_resp.calls
        assert 1 == len(calls)
        assert {
            "cursor": None,
            "limit": 25,
            "filter": {
                "scheduleId": 395335920687780,
                "status": "Completed",
            },
        } == jsgz_load(calls[0].request.body)


class TestFunctionCallLog:
    @pytest.fixture(scope="class")
    def entries(self) -> list[tuple[datetime.datetime, str]]:
        start_ts = datetime.datetime(2023, 10, 4, 10, 30, 4, 123000, tzinfo=datetime.timezone.utc)
        ms_delta = datetime.timedelta(milliseconds=100)
        return [(start_ts + i * ms_delta, f"line {i}") for i in range(10)]

    def test_to_text_without_timestamps(self, entries):
        log = FunctionCallLog(resources=[FunctionCallLogEntry(timestamp=ts, message=msg) for (ts, msg) in entries])
        expected = "\n".join([msg for (_, msg) in entries])
        assert log.to_text() == expected

    def test_to_text_with_timestamps(self, entries):
        log = FunctionCallLog(
            resources=[FunctionCallLogEntry(timestamp=datetime_to_ms(ts), message=msg) for (ts, msg) in entries]
        )
        expected = "\n".join(entry._format(with_timestamps=True) for entry in log)
        assert log.to_text(with_timestamps=True) == expected


class TestFunctionCallLogEntry:
    def test_format_with_timestamp(self):
        ts = datetime.datetime(2023, 10, 4, 10, 30, 4, 123000, tzinfo=datetime.timezone.utc)
        entry = FunctionCallLogEntry(timestamp=datetime_to_ms(ts), message="line one")

        assert entry._format(with_timestamps=True) == f"[{ts}] line one"

    def test_format_without_timestamp(self):
        ts = datetime.datetime(2023, 10, 4, 10, 30, 4, 123)
        entry = FunctionCallLogEntry(timestamp=datetime_to_ms(ts), message="line one")

        assert entry._format(with_timestamps=False) == "line one"

    def test_format_with_none_timestamp(self):
        entry = FunctionCallLogEntry(timestamp=None, message="line one")

        assert entry._format(with_timestamps=True) == "line one"
        assert entry._format(with_timestamps=False) == "line one"
