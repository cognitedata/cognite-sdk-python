import re
from unittest.mock import MagicMock, call

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import Function, FunctionSchedule
from tests.utils import jsgz_load


@pytest.fixture
def mock_client():
    # We allow the mock to pass isinstance checks
    (client := MagicMock()).__class__ = CogniteClient
    return client


@pytest.fixture
def empty_function(mock_client):
    return Function(id=123, cognite_client=mock_client)


@pytest.fixture
def function(mock_client):
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
        cognite_client=mock_client,
    )


@pytest.fixture
def function_schedules(mock_client):
    schedule_1 = FunctionSchedule(
        name="my-schedule-1",
        function_id=123,
        description="my schedule 1",
        created_time=12345,
        cron_expression="* * * * *",
        cognite_client=mock_client,
    )
    schedule_2 = FunctionSchedule(
        name="my-schedule-2",
        function_external_id="my-function",
        description="my schedule 2",
        created_time=12345,
        cron_expression="* * * * *",
        cognite_client=mock_client,
    )
    return [schedule_1, schedule_2]


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

    @pytest.mark.parametrize("limit", [2, None, -1, float("inf")])
    def test_list_schedules(self, function, function_schedules, limit):
        function._cognite_client.functions.schedules._LIST_LIMIT_CEILING = 10
        function._cognite_client.functions.schedules.list.side_effect = [
            [function_schedules[0]],
            [function_schedules[1]],
        ]

        schedules = function.list_schedules(limit=limit)

        calls = [
            call(function_external_id=function.external_id, limit=limit),
            call(function_id=function.id, limit=limit),
        ]

        assert 2 == len(schedules)
        function._cognite_client.functions.schedules.list.assert_has_calls(calls)


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
