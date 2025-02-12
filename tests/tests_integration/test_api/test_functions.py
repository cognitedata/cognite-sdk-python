from __future__ import annotations

from contextlib import suppress

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import Function, FunctionList, FunctionSchedule, FunctionScheduleWrite
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError


def handle(client, data) -> str:
    return "Hello, world!"


@pytest.fixture(scope="module")
def a_function(cognite_client: CogniteClient) -> Function:
    name = "python-sdk-test-function"
    external_id = "python-sdk-test-function"
    description = "test function"
    retrieved = cognite_client.functions.retrieve_multiple(external_ids=[external_id], ignore_unknown_ids=True)
    if retrieved:
        return retrieved[0]

    function = cognite_client.functions.create(
        name=name,
        external_id=external_id,
        description=description,
        function_handle=handle,
    )
    return function


@pytest.fixture(scope="session")
def dummy_function(cognite_client: CogniteClient) -> Function:
    name = "integration_test-dummy-noxid-1"
    retrieved = cognite_client.functions.list(name=name, limit=1)
    if retrieved:
        return retrieved[0]

    def handle(client, data, secrets, function_call_info):
        print(f"Inputs: {data!r}")  # noqa
        print(f"Call info: {function_call_info!r}")  # noqa
        return data

    created = cognite_client.functions.create(
        name=name, function_handle=handle, description="print inputs & call info, return inputs"
    )
    schedules = [
        FunctionScheduleWrite(
            name="once-a-year-05-13",
            cron_expression="36 2 13 5 *",
            function_id=created.id,
            data={"day": "05-13", "scheduled": True},
        ),
        FunctionScheduleWrite(
            name="once-a-year-04-03",
            cron_expression="6 4 3 4 *",
            function_id=created.id,
            data={"day": "04-03", "scheduled": True},
        ),
        FunctionScheduleWrite(
            name="once-a-year-06-06",
            cron_expression="50 1 6 6 *",
            function_id=created.id,
            data={"day": "06-06", "scheduled": True},
        ),
        FunctionScheduleWrite(
            name="once-a-year-06-24",
            cron_expression="14 6 24 6 *",
            function_id=created.id,
            data={"day": "06-24", "scheduled": True},
        ),
        FunctionScheduleWrite(
            name="once-a-year-05-25",
            cron_expression="36 6 25 5 *",
            function_id=created.id,
            data={"day": "05-25", "scheduled": True},
        ),
    ]
    for schedule in schedules:
        cognite_client.functions.schedules.create(schedule)
    return created


class TestFunctionsAPI:
    def test_retrieve_unknown_raises_error(self, cognite_client: CogniteClient):
        with pytest.raises(CogniteNotFoundError) as e:
            cognite_client.functions.retrieve_multiple(external_ids=["this does not exist"])

        assert e.value.not_found[0]["external_id"] == "this does not exist"

    def test_retrieve_unknown_ignore_unknowns(self, cognite_client: CogniteClient):
        res = cognite_client.functions.retrieve_multiple(external_ids=["this does not exist"], ignore_unknown_ids=True)
        assert len(res) == 0

    def test_function_list_schedules_unlimited(self, cognite_client: CogniteClient, dummy_function: Function):
        expected_unique_schedules = 5
        # This is an integration test dummy function that purposefully doesn't have an external id.
        fn = cognite_client.functions.retrieve(id=dummy_function.id)
        schedules = fn.list_schedules(limit=-1)

        assert len(schedules) == expected_unique_schedules
        assert len({s.id for s in schedules}) == expected_unique_schedules
        assert len({s.cron_expression for s in schedules}) == expected_unique_schedules

    def test_create_schedule_with_bad_external_id(self, cognite_client: CogniteClient):
        xid = "bad_xid"
        with pytest.raises(ValueError, match=f'Function with external ID "{xid}" is not found'):
            cognite_client.functions.schedules.create(
                function_external_id=xid,
                cron_expression="* * * * *",
                name="test_schedule",
            )

    def test_iterate_functions(self, cognite_client: CogniteClient) -> None:
        for function in cognite_client.functions:
            assert isinstance(function, Function)
            break
        else:
            assert False, "Expected at least one function"

    def test_iterate_chunked_functions(self, cognite_client: CogniteClient) -> None:
        for function in cognite_client.functions(chunk_size=2):
            assert isinstance(function, FunctionList)
            assert len(function) <= 2
            break
        else:
            assert False, "Expected at least one function"

    def test_call_function_with_oneshot(self, cognite_client: CogniteClient, a_function: Function) -> None:
        session = cognite_client.iam.sessions.create(session_type="ONESHOT_TOKEN_EXCHANGE")

        response = cognite_client.functions.call(id=a_function.id, wait=True, nonce=session.nonce)

        assert response.status == "Completed"


class TestFunctionSchedulesAPI:
    def test_create_retrieve_delete(self, cognite_client: CogniteClient, a_function: Function) -> None:
        my_schedule = FunctionScheduleWrite(
            name="python-sdk-test-schedule",
            cron_expression="0 0 1 1 *",
            function_external_id=a_function.external_id,
            data={"key": "value"},
        )
        original_schedule = FunctionScheduleWrite._load(my_schedule.dump())

        created: FunctionSchedule | None = None
        try:
            created = cognite_client.functions.schedules.create(my_schedule)

            created_dump = created.as_write().dump()
            created_dump.pop("functionId")
            my_dump = my_schedule.dump()
            my_dump.pop("functionExternalId")

            assert created_dump == my_dump
            # This check is to ensure that the original schedule is not modified
            assert my_schedule.dump() == original_schedule.dump()

            retrieved = cognite_client.functions.schedules.retrieve(id=created.id)
            assert isinstance(retrieved, FunctionSchedule)
            assert retrieved.dump() == created.dump()

            cognite_client.functions.schedules.delete(id=created.id)
        finally:
            if created:
                with suppress(CogniteAPIError):
                    cognite_client.functions.schedules.delete(id=created.id)

    def test_retrieve_unknown(self, cognite_client: CogniteClient) -> None:
        # The ID 123 should not exist
        retrieved = cognite_client.functions.schedules.retrieve(id=123)

        assert retrieved is None

    def test_retrieve_known_and_unknown(self, cognite_client: CogniteClient, a_function: Function) -> None:
        my_schedule = FunctionScheduleWrite(
            name="python-sdk-test-schedule-known",
            cron_expression="0 0 1 1 *",
            function_id=a_function.id,
            data={"key": "value"},
        )
        created: FunctionSchedule | None = None
        try:
            created = cognite_client.functions.schedules.create(my_schedule)

            retrieved = cognite_client.functions.schedules.retrieve([created.id, 123], ignore_unknown_ids=True)
            assert len(retrieved) == 1
            assert retrieved[0].id == created.id
        finally:
            if created:
                with suppress(CogniteAPIError):
                    cognite_client.functions.schedules.delete(id=created.id)

    def test_raise_retrieve_unknown(self, cognite_client: CogniteClient) -> None:
        with pytest.raises(CogniteNotFoundError):
            cognite_client.functions.schedules.retrieve(id=[123])
