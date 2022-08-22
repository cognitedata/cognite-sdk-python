from unittest.mock import MagicMock, call

import pytest

from cognite.client.data_classes import Function, FunctionSchedule


@pytest.fixture
def empty_function():
    return Function(id=123, cognite_client=MagicMock())


@pytest.fixture
def function():
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
        cognite_client=MagicMock(),
    )


@pytest.fixture
def function_schedules():
    schedule_1 = FunctionSchedule(
        name="my-schedule-1",
        function_id=123,
        description="my schedule 1",
        created_time=12345,
        cron_expression="* * * * *",
        cognite_client=MagicMock(),
    )
    schedule_2 = FunctionSchedule(
        name="my-schedule-2",
        function_external_id="my-function",
        description="my schedule 2",
        created_time=12345,
        cron_expression="* * * * *",
        cognite_client=MagicMock(),
    )
    return [schedule_1, schedule_2]


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
